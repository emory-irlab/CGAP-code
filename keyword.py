import sys
import time

from google.ads.google_ads.client import GoogleAdsClient
from google.ads.google_ads.errors import GoogleAdsException

import trends
from universal import *

# STATIC VARIABLES
AVG_MONTHLY_SEARCH: str = "avg_monthly_search"
COMPETITION_VALUE: str = "competition_value"
CUSTOMER_ID: str = "customer_id"
EXPANSION: str = "expansion"
EXPANDED_FROM: str = "expanded_from"

# DEFAULT
DEFAULT_LANGUAGE_ID_ENGLISH: str = "1000"
DEFAULT_SOURCE_PRIORITY_ORDER: Tuple[str, ...] = (
	"cgap",
)

# PARAMETERS
PARAM_SOURCE_PRIORITY_ORDER: str = "source_priority_order"

# NAMED TUPLES
NT_filename_expansion_raw = namedtuple(
	"NT_filename_expansion_raw",
	[
		CITY,
		KEYWORD,
	]
)
NT_filename_expansion_parents = namedtuple(
	"NT_filename_expansion_parents",
	[
		"expanded_keyword",
	]
)
NT_filename_keywords_google = namedtuple(
	"NT_filename_keywords_google",
	[
		"seed_keyword",
	]
)


def main(
		called_from_main: bool = False,
		list_cities: Tuple[str, ...] = tuple(DEFAULT_CITIES),
		partition_group: int = 1,
		partition_total: int = 1,
) -> None:
	set_error_file_origin(KEYWORD)
	set_error_folder(FOLDER_ERROR)
	set_partition_group(partition_group)
	set_partition_total(partition_total)
	with open(f"{KEYWORD}{HYPHEN}{PARAMETERS}{JSON}") as json_file:
		json_data: dict = json.load(json_file)
		download: bool
		only_download_missing: bool
		aggregate: bool
		customer_id: str
		credentials: str
		list_partitioned_cities: Tuple[str, ...]
		list_source_priority_order: List[str]
		if called_from_main:
			download = json_data[DOWNLOAD]
			only_download_missing = json_data[PARAM_ONLY_DOWNLOAD_MISSING]
			aggregate = json_data[AGGREGATE]
			credentials = json_data[PARAM_CREDENTIALS]
			customer_id = json_data[CUSTOMER_ID]
			parameters: dict = json_data[KEYWORD]
			list_source_priority_order = json_data[PARAM_SOURCE_PRIORITY_ORDER]
			list_input_cities: List[str] = parameters[CITY]
			list_input_cities.sort()
			list_partitioned_cities = tuple(
				partition_list(
					list_partition_candidates=list_input_cities,
					partition_group=get_partition_group(),
					partition_total=get_partition_total(),
				)
			)
		else:
			list_partitioned_cities = list_cities
			only_download_missing = True
			download = False
			aggregate = False
			customer_id = ""
			credentials = ""
			list_source_priority_order = []
	json_file.close()

	google_ads_client: GoogleAdsClient = GoogleAdsClient.load_from_storage(credentials)

	if download:
		set_error_task_origin(task_origin=DOWNLOAD)
		city: str
		for city in list_partitioned_cities:
			download_expansion(
				city=city,
				client=google_ads_client,
				customer_id=customer_id,
				only_expand_missing=only_download_missing,
				folder_expansion_raw=FOLDER_EXPANSION_RAW,
				folder_expansion_parents=FOLDER_EXPANSION_PARENTS,
				folder_keywords_google=FOLDER_KEYWORDS_GOOGLE,
				list_source_priority_order=tuple(list_source_priority_order),
			)
			write_errors_to_disk(overwrite=False)

	if aggregate:
		set_error_task_origin(task_origin=AGGREGATE)
		is_valid_for_aggregation = check_partition_valid_for_aggregation(
			error_label=AGGREGATE,
			partition_group=get_partition_group(),
			partition_total=get_partition_total(),
		)
		if is_valid_for_aggregation:
			source: str
			for source in list_source_priority_order:
				log_error(f"{AGGREGATE} : {SOURCE} : {source}", log=True)
				aggregate_data_in_folder(
					filename_label=source,
					folder_input=generate_source_folder(
						source=source,
						folder=FOLDER_EXPANSION_RAW,
					),
					folder_output_aggregate=FOLDER_EXPANSION_AGGREGATE,
					list_cities=list_partitioned_cities,
					bool_suppress_print=True,
				)
				write_errors_to_disk(clear_task_origin=False, bool_suppress_print=True, overwrite=False)

			log_error(f"{AGGREGATE} : {EXPANSION}", log=True)
			aggregate_data_in_folder(
				filename_label=KEYWORD,
				folder_input=FOLDER_EXPANSION_AGGREGATE,
				folder_output_aggregate=FOLDER_EXPANSION_AGGREGATE,
				list_cities=list_partitioned_cities,
			)
		write_errors_to_disk(overwrite=False)


def download_expansion(
		city: str,
		client: GoogleAdsClient,
		customer_id: str,
		only_expand_missing: bool,
		folder_expansion_raw: str,
		folder_expansion_parents: str,
		folder_keywords_google: str,
		list_source_priority_order: Tuple[str] = DEFAULT_SOURCE_PRIORITY_ORDER,
		folder_keywords: str = FOLDER_KEYWORDS,
		language_id: str = DEFAULT_LANGUAGE_ID_ENGLISH,
) -> None:
	location_id: str = DEFAULT_CITIES.get(city, {}).get(GOOGLE_GEO_CODE, "")
	list_location_ids: List[str] = [location_id]

	keyword_plan_idea_service = client.get_service("KeywordPlanIdeaService", version="v3")
	keyword_competition_level_enum = (
		client.get_type("KeywordPlanCompetitionLevelEnum", version="v3").KeywordPlanCompetitionLevel
	)
	keyword_plan_network = client.get_type(
		"KeywordPlanNetworkEnum", version="v3"
	).GOOGLE_SEARCH
	locations = map_locations_to_string_values(
		client=client,
		location_ids=list_location_ids,
	)
	language = map_language_to_string_value(
		client=client,
		language_id=language_id,
	)

	url_seed = None
	keyword_url_seed = None

	dict_keywords: dict = trends.generate_keywords(
		folder_keywords=folder_keywords,
	)

	source: str
	for source in list_source_priority_order:
		folder_expansion_raw_source: str = generate_source_folder(
			source=source,
			folder=folder_expansion_raw,
		)
		list_source_keywords: List[str] = dict_keywords[source]
		list_already_expanded_filenames_in_raw_source: List[str] = import_paths_from_folder(
			folder=folder_expansion_raw_source,
			list_paths_filter_conditions=(city,),
		)

		seed_keyword: str
		for seed_keyword in list_source_keywords:
			nt_filename_expansion_raw: tuple = NT_filename_expansion_raw(
				city=city,
				keyword=seed_keyword,
			)
			filename_expansion_raw: str = generate_filename(
				nt_filename=nt_filename_expansion_raw,
				delimiter=HYPHEN,
				extension=CSV,
			)

			if only_expand_missing and filename_expansion_raw in list_already_expanded_filenames_in_raw_source:
				continue

			print(f"expansion : {city} : {seed_keyword}")
			keyword_seed = client.get_type("KeywordSeed", version="v3")
			keyword_protos = map_keywords_to_string_values(
				client=client,
				keywords=[seed_keyword],
			)
			keyword_seed.keywords.extend(keyword_protos)
			time.sleep(2)
			try:
				keyword_ideas = keyword_plan_idea_service.generate_keyword_ideas(
					customer_id,
					language,
					locations,
					keyword_plan_network,
					url_seed=url_seed,
					keyword_seed=keyword_seed,
					keyword_and_url_seed=keyword_url_seed,
				)
			except GoogleAdsException as ex:
				log_error(error=f"{city}{HYPHEN}{seed_keyword}{HYPHEN}exception")
				log_exception(exception=ex)
				write_errors_to_disk(clear_task_origin=False, overwrite=False)
				continue

			list_expanded_keywords: List[str] = []
			list_individual_keyword_ideas_results: List[pd.DataFrame] = []
			for idea in keyword_ideas.results:
				dict_keyword_ideas_result = {}
				dict_keyword_ideas_result.update({CITY: city})
				expanded_keyword = idea.text.value
				dict_keyword_ideas_result.update({KEYWORD: expanded_keyword})
				dict_keyword_ideas_result.update({SOURCE: source})
				list_expanded_keywords.append(f"{city}{HYPHEN}{expanded_keyword}")
				dict_keyword_ideas_result.update(
					{AVG_MONTHLY_SEARCH: idea.keyword_idea_metrics.avg_monthly_searches.value}
				)
				competition_value = keyword_competition_level_enum.Name(idea.keyword_idea_metrics.competition)
				dict_keyword_ideas_result.update({COMPETITION_VALUE: competition_value})
				list_individual_keyword_ideas_results.append(
					pd.DataFrame(
						dict_keyword_ideas_result,
						index=[0],
					)
				)

				filename_expansion_parent: str = generate_filename(
					nt_filename=NT_filename_expansion_parents(
						expanded_keyword=expanded_keyword,
					),
					extension=TXT,
				)
				write_list_to_file(
					filename=f"{filename_expansion_parent}",
					folder=folder_expansion_parents,
					list_strings=[f"{city}{HYPHEN}{seed_keyword}"],
				)

			output_keywords_google_filename: str = generate_filename(
				nt_filename=NT_filename_keywords_google(
					seed_keyword=seed_keyword,
				),
				extension=TXT,
			)
			write_list_to_file(
				filename=f"{output_keywords_google_filename}",
				folder=generate_source_folder(
					source=source,
					folder=folder_keywords_google,
				),
				list_strings=list_expanded_keywords,
			)
			df_keyword_ideas_for_city: pd.DataFrame
			if len(list_individual_keyword_ideas_results) > 0:
				df_keyword_ideas_for_city = pd.concat(
					list_individual_keyword_ideas_results,
					ignore_index=True,
				)
			else:
				df_keyword_ideas_for_city = pd.DataFrame()
				log_error(error=f"{city}{HYPHEN}{seed_keyword}{HYPHEN}{ERROR_EMPTY}")
			df_keyword_ideas_for_city.to_csv(
				f"{folder_expansion_raw_source}{filename_expansion_raw}",
				index=False,
			)


def generate_source_folder(
		source: str,
		folder: str,
) -> str:
	generate_sub_paths_for_folder(
		folder=folder,
	)
	return f"{folder}{source}{FORWARD_SLASH}"


def map_keywords_to_string_values(
		client: GoogleAdsClient,
		keywords: List[str],
) -> List:
	keyword_protos: List = []
	keyword: str
	for keyword in keywords:
		# google.protobuf.StringValue
		string_val = client.get_type("StringValue")
		string_val.value = keyword
		keyword_protos.append(string_val)
	return keyword_protos


def map_locations_to_string_values(
		client: GoogleAdsClient,
		location_ids: List[str],
) -> List:
	gtc_service = client.get_service("GeoTargetConstantService", version="v3")
	locations = []
	location_id: str
	for location_id in location_ids:
		# google.protobuf.StringValue
		location = client.get_type("StringValue")
		location.value = gtc_service.geo_target_constant_path(location_id)
		locations.append(location)
	return locations


def map_language_to_string_value(
		client: GoogleAdsClient,
		language_id: str,
) -> Any:
	# google.protobuf.StringValue
	language = client.get_type("StringValue")
	language.value = client.get_service("LanguageConstantService", version="v3").language_constant_path(language_id)
	return language


def log_exception(
		exception: GoogleAdsException,
) -> None:
	log_error(error=f"Request with ID {exception.request_id} failed with status {exception.error.code().name} and includes the following errors:")
	for error in exception.failure.errors:
		log_error(error=f"Error with message {error.message}.")
		if error.location:
			for field_path_element in error.location.field_path_elements:
				log_error(error=f"On field: {field_path_element.field_name}")


if __name__ == "__main__":
	called_from_main = True
	if len(sys.argv) == 3:
		partition_group: int = int(sys.argv[1])
		partition_total: int = int(sys.argv[2])
		main(
			called_from_main=called_from_main,
			partition_group=partition_group,
			partition_total=partition_total,
		)
	else:
		main(called_from_main=called_from_main)
