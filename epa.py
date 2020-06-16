import sys
import requests

from universal import *

# STATIC VARIABLES
DATE_EPA: str = 'Date'
OZONE_DONGHAI: str = 'Ozone'
EPA_API_CBSA: str = 'cbsa'
EPA_API_DATE_FORMAT: str = '%Y%m%d'
EPA_API_START_DATE: str = 'bdate'
EPA_API_END_DATE: str = 'edate'

# PARAMS
PARAM_DOWNLOAD_DATA_TYPE: str = "download_data_type"
PARAM_ONLY_DOWNLOAD_MISSING: str = "only_download_missing"

# NAMED TUPLES
# NAMED TUPLES
NT_filename_epa_raw = namedtuple(
	'NT_filename_epa_raw',
	[
		CITY,
		POLLUTANT,
		START_DATE,
		END_DATE,
	]
)
NT_filename_epa_stitch = namedtuple(
	'NT_filename_epa_stitch',
	[
		CITY,
		POLLUTANT,
		TARGET_STATISTIC,
		START_DATE,
		END_DATE,
	]
)


def main(
		called_from_main: bool = False,
		list_cities: Tuple[str, ...] = tuple(DEFAULT_CITIES),
		partition_group: int = 1,
		partition_total: int = 1,
) -> None:
	set_error_file_origin(EPA)
	set_error_folder(FOLDER_ERROR)
	set_partition_group(partition_group)
	set_partition_total(partition_total)
	with open(f'{EPA}{HYPHEN}{PARAMETERS}{JSON}') as json_file:
		json_data = json.load(json_file)
		aggregate: bool
		download: bool
		download_data_type: str
		only_download_missing: bool
		stitch: bool
		only_stitch_missing: bool
		list_partitioned_cities: Tuple[str, ...]
		list_pollutants: Tuple[str, ...]
		if called_from_main:
			aggregate = json_data[AGGREGATE]
			download = json_data[DOWNLOAD]
			download_data_type = json_data[PARAM_DOWNLOAD_DATA_TYPE]
			only_download_missing = json_data[PARAM_ONLY_DOWNLOAD_MISSING]
			stitch = json_data[STITCH]
			credentials: str = json_data[CREDENTIALS]
			parameters: dict = json_data[EPA]
			api: dict = json_data[API]
			list_input_cities: List[str] = parameters[CITY]
			list_input_cities.sort()
			list_partitioned_cities = tuple(
				partition_list(
					list_partition_candidates=list_input_cities,
					partition_group=get_partition_group(),
					partition_total=get_partition_total(),
				)
			)
			list_pollutants = parameters[POLLUTANT]
		else:
			aggregate = False
			download = False
			download_data_type = ""
			only_download_missing = True
			stitch = True
			list_partitioned_cities = list_cities
	json_file.close()

	if download:
		set_error_task_origin(task_origin=DOWNLOAD)
		api_url: str = api.get(download_data_type, "")
		if api_url:
			api_starter_params: dict = parse_api_credentials(filename=credentials)
			city: str
			for city in list_partitioned_cities:
				for pollutant in list_pollutants:
					download_epa(
						city=city,
						pollutant=pollutant,
						list_date_pairs=LIST_DATE_PAIRS,
						api_url=api_url,
						api_starter_params=api_starter_params,
						download_data_type=download_data_type,
						only_download_missing=only_download_missing,
						folder_epa_raw=FOLDER_EPA_RAW,
					)
		else:
			log_error(error=)  # todo missing api link
		write_errors_to_disk(
			clear_task_origin=False,
			overwrite=False,
		)

	if stitch:
		set_error_task_origin(task_origin=STITCH)
		city: str
		for city in list_partitioned_cities:
			# todo - only stitch missing
			# todo - include pollutant as param
			# start_date, end_date = generate_date_pair_for_full_series(list_date_pairs)
			stitch_epa(
				city=city,
				folder_epa_raw=FOLDER_EPA_RAW,
				folder_epa_stitch=FOLDER_EPA_STITCH,
			)
		# todo - overwrite based on only stitch missing
		write_errors_to_disk(overwrite=(not only_stitch_missing))

	if aggregate:
		set_error_task_origin(task_origin=AGGREGATE)
		is_valid_for_aggregation: bool = check_partition_valid_for_aggregation(
			error_label=EPA,
			partition_group=get_partition_group(),
			partition_total=get_partition_total(),
		)
		if is_valid_for_aggregation:
			generate_sub_paths_for_folder(
				folder=FOLDER_EPA_AGGREGATE,
			)
			df_aggregate_epa: pd.DataFrame = aggregate_epa(
				list_cities=tuple(DEFAULT_CITIES),
				folder_epa_stitch=FOLDER_EPA_STITCH,
				folder_epa_aggregate=FOLDER_EPA_AGGREGATE,
			)
			output_epa_aggregate_filename: str = generate_filename(
				filename_nt=NT_aggregate_filename,
				extension=CSV,
			)
			df_aggregate_epa.to_csv(
				f'{FOLDER_EPA_AGGREGATE}{output_epa_aggregate_filename}',
				index=False,
			)
		write_errors_to_disk()


def generate_api_parameters(
		api_call_type: str,
) -> {}:
	if api_call_type == CBSA:

		return {}
	else:
		return {}


def download_epa(
		city: str,
		pollutant: str,
		list_date_pairs: List[Tuple[str, str]],
		api_url: str,
		api_starter_params: dict,
		download_data_type: str,
		only_download_missing: bool = True,
		folder_epa_raw: str = FOLDER_EPA_RAW,
) -> None:
	api_params: dict = api_starter_params.copy()

	pollutant_dict: dict = DEFAULT_POLLUTANTS.get(pollutant, "")
	if pollutant_dict:
		param: str = pollutant_dict.get(EPA_API_POLLUTANT_PARAM, "")
		if param:
			api_params.update({EPA_API_POLLUTANT_PARAM: param})
		else:
			log_error()
			return
	else:
		log_error()
		return

	if download_data_type == CBSA:
		city_dict: dict = DEFAULT_CITIES.get(city, {})
		if city_dict:
			cbsa: int = city_dict.get(CBSA, 0)
			if cbsa:
				api_params.update({EPA_API_CBSA: cbsa})
			else:
				log_error()
				return
		else:
			log_error()
			return

		start_date_str: str
		end_date_str: str
		for start_date_str, end_date_str in list_date_pairs:
			start_date_dt: datetime = datetime.strptime(start_date_str, DATE_FORMAT)
			end_date_dt: datetime = datetime.strptime(end_date_str, DATE_FORMAT)
			api_params.update({EPA_API_START_DATE: start_date_dt.strftime(EPA_API_DATE_FORMAT)})
			api_params.update({EPA_API_END_DATE: end_date_dt.strftime(EPA_API_DATE_FORMAT)})
			response: requests.Response = requests.get(
				url=api_url,
				params=api_params,
			)
			print(response)
			print(response.content)
			exit()

	# todo
	# setup file naming convention for epa raw
	# set up stitching for epa


def aggregate_epa(
		list_cities=tuple(DEFAULT_CITIES),
		folder_epa_stitch=FOLDER_EPA_STITCH,
		folder_epa_aggregate=FOLDER_EPA_AGGREGATE,
) -> pd.DataFrame:
	set_error_task_origin(task_origin=AGGREGATE)
	generate_sub_paths_for_folder(
		folder=folder_epa_aggregate,
	)
	list_city_dfs: List[pd.DataFrame] = []
	city: str
	for city in list_cities:
		log_error(f'{AGGREGATE} : {city}', log=True)
		list_parsed_epa_df_for_city: List[pd.DataFrame] = []
		list_stitched_epa_filenames: List[str] = import_paths_from_folder(
			folder=folder_epa_stitch,
			list_paths_filter_conditions=(city, CSV),
		)
		if len(list_stitched_epa_filenames) > 0:
			filename: str
			for filename in list_stitched_epa_filenames:
				df: pd.DataFrame = pd.read_csv(
					f'{folder_epa_stitch}{filename}',
					parse_dates=[DATE],
				)
				parsed_city: str
				pollutant: str
				target_statistic: str
				parsed_city, pollutant, target_statistic = filename.replace(CSV, EMPTY_STRING).split(HYPHEN)
				df.insert(0, CITY, city)
				df.insert(1, POLLUTANT, pollutant)
				df.insert(2, TARGET_STATISTIC, target_statistic)
				list_parsed_epa_df_for_city.append(df)

			df_parsed_city: pd.DataFrame = pd.concat(
				list_parsed_epa_df_for_city,
				sort=True,
			)
			list_city_dfs.append(df_parsed_city)
			output_aggregate_for_city_filename: str = generate_filename(
				filename_nt=NT_city_aggregate_filename(
					city=city,
				),
				extension=CSV,
			)
			df_parsed_city.to_csv(
				f'{folder_epa_aggregate}{output_aggregate_for_city_filename}',
				index=False,
			)

	return pd.concat(
		list_city_dfs,
		sort=True,
	)


def stitch_epa(
		city: str,
		epa_column_name: str = POLLUTION_LEVEL,
		folder_epa_raw: str = FOLDER_EPA_RAW,
		folder_epa_stitch: str = FOLDER_EPA_STITCH,
) -> None:
	log_error(f'{STITCH} : {city}', log=True)

	generate_sub_paths_for_folder(
		folder=folder_epa_stitch,
	)
	filename: str = import_single_file(
		folder=folder_epa_raw,
		list_filename_filter_conditions=(city, CSV),
	)
	if filename:
		df_city: pd.DataFrame = pd.read_csv(
			f'{folder_epa_raw}{filename}',
			parse_dates=[DATE_EPA],
		)
		df_city.rename(columns={DATE_EPA: DATE}, inplace=True)
		df_city.set_index(DATE, inplace=True)
		df_city.drop(columns=UNNAMED, inplace=True)

		column_name: str
		list_column_names: List[str] = list(df_city.columns)
		for column_name in list_column_names:
			parsed_city: str
			pollutant: str
			target_statistic: str
			parsed_city, pollutant, target_statistic = parse_column_name(column_name)
			if parsed_city != city:
				log_error(error=f'city_mismatch{HYPHEN}{city}{HYPHEN}{parsed_city}')
			df_single_column: pd.DataFrame = df_city[column_name].to_frame(name=epa_column_name)
			nt_epa_stitch_filename: tuple = NT_filename_epa_stitch(
				city=city,
				pollutant=pollutant,
				target_statistic=target_statistic,
			)
			output_epa_stitch_filename: str = generate_filename(
				filename_nt=nt_epa_stitch_filename,
				extension=CSV,
				delimiter=HYPHEN,
			)
			df_single_column.to_csv(f'{folder_epa_stitch}{output_epa_stitch_filename}')
	else:
		log_error(error=f'city_not_found{HYPHEN}{city}')


def parse_column_name(column_name: str) -> Tuple[str, str, str]:
	city_code: str
	pollutant_code: str
	target_statistic: str
	city_code, pollutant_code, target_statistic = column_name.split(UNDERSCORE)
	city_name: str = parse_city_code(city_code)
	pollutant: str = parse_pollutant(pollutant_code)

	return city_name, pollutant, target_statistic


def parse_city_code(city_code: str) -> str:
	city_name: str
	city_info: dict
	for city_name, city_info in DEFAULT_CITIES.items():
		if city_code == city_info.get(CITY_AB, EMPTY_STRING):
			return city_name
	log_error(error=f'city_code_not_found{HYPHEN}{city_code}')
	return EMPTY_STRING


def parse_pollutant(
		pollutant: str,
		list_pollutants: Tuple[str, ...] = tuple(DEFAULT_POLLUTANTS),
) -> str:
	if pollutant == OZONE_DONGHAI:
		pollutant = O3

	if pollutant in list_pollutants:
		return pollutant
	else:
		log_error(error=f'pollutant_not_found{HYPHEN}{pollutant}')
		return EMPTY_STRING


if __name__ == '__main__':
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
