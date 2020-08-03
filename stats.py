import sys

import numpy as np

import epa
import trends
from universal import *

# STATIC VARIABLES
CGAP: str = "cgap"
COMMON_CITY: str = "common_city"
CORRELATIONS_COMPARISON: str = "correlations_comparison"
CORRELATE_ABOVE_THRESHOLD: str = "above_threshold"
CORRELATE_BELOW_THRESHOLD: str = "below_threshold"
DATA_FRAME: str = "data_frame"

METRICS: str = "metrics"
SPARSITY: str = "sparsity"
STATS: str = "stats"
TARGET_VARIABLE_COLUMN_NAME: str = "target_variable_column_name"

# PARAMETERS
PARAM_AGGREGATE_CORRELATIONS: str = "aggregate_correlations"
PARAM_AGGREGATE_INTERCITY: str = "aggregate_intercity"
PARAM_AGGREGATE_METRICS_EPA: str = f"{AGGREGATE}{UNDERSCORE}{EPA}{UNDERSCORE}{METRICS}"
PARAM_AGGREGATE_METRICS_TRENDS = f"{AGGREGATE}{UNDERSCORE}{TRENDS}{UNDERSCORE}{METRICS}"
PARAM_AGGREGATE_ALREADY_AGGREGATED_CITIES: str = "aggregate_already_aggregated_cities"
PARAM_CORRELATE: str = "correlate"
PARAM_CORRELATE_ABOVE_THRESHOLD: str = "correlate_above_threshold"
PARAM_CORRELATE_BELOW_THRESHOLD: str = "correlate_below_threshold"
PARAM_INTERCITY: str = "run_intercity"
PARAM_ONLY_CORRELATE_MISSING: str = "only_correlate_missing"
PARAM_METRICS_EPA: str = f"run{UNDERSCORE}{EPA}{UNDERSCORE}{METRICS}"
PARAM_METRICS_TRENDS: str = f"run{UNDERSCORE}{TRENDS}{UNDERSCORE}{METRICS}"
PARAM_STITCH_EPA: str = f"{STITCH}{UNDERSCORE}{EPA}"
PARAM_STITCH_TRENDS: str = f"{STITCH}{UNDERSCORE}{TRENDS}"
PARAM_UPLOAD_AGGREGATE_FROM_FOLDER: str = "upload_aggregate_from_folder"
PARAM_UPLOAD_FOLDER: str = "upload_folder"

# DEFAULT
DEFAULT_COMMON_CITY: str = USA
DEFAULT_IGNORE_ZERO_EPA: bool = False
DEFAULT_ONLY_CORRELATE_MISSING: bool = True
DEFAULT_ONLY_COMPARE_MISSING: bool = True
DEFAULT_IGNORE_ZERO_TRENDS: Tuple[bool, ...] = (
	True,
	False,
)

DEFAULT_THRESHOLD_SIDES: Tuple[str, ...] = (
	CORRELATE_ABOVE_THRESHOLD,
	CORRELATE_BELOW_THRESHOLD,
)

DEFAULT_TIME_SHIFTS: Tuple[int, ...] = (
	-1,
	0,
	1,
	2,
	3,
)

# Named Tuples
NT_filename_mean_max_epa = namedtuple(
	"NT_filename_mean_max_epa",
	[
		CITY,
		POLLUTANT,
		EPA_COLUMN_SITE_NUMBER,
	]
)
NT_filename_metrics_epa = namedtuple(
	"NT_filename_metrics_epa",
	[
		CITY,
		POLLUTANT,
		EPA_COLUMN_SITE_NUMBER,
		TARGET_STATISTIC,
		IGNORE_ZERO,
		YEAR,
	]
)
NT_filename_metrics_trends = namedtuple(
	"NT_filename_metrics_trends",
	[
		CITY,
		COMMON_WORD,
		KEYWORD,
		IGNORE_ZERO,
		YEAR,
	]
)
NT_filename_intercity = namedtuple(
	"NT_filename_intercity",
	[
		COMMON_CITY,
		COMMON_WORD,
		KEYWORD,
		CITY,
	]
)
NT_filename_correlation = namedtuple(
	"NT_filename_correlation",
	[
		CITY,
		KEYWORD,
		POLLUTANT,
		EPA_COLUMN_SITE_NUMBER,
		TARGET_STATISTIC,
		IGNORE_ZERO,
		THRESHOLD,
		THRESHOLD_SIDE,
		TIME_SHIFT,
	]
)
DT_filename_correlation = {
	IGNORE_ZERO: "bool",
	THRESHOLD:   (lambda x: parse_filename_numeric(numeric=x, cast_type="float")),
	TIME_SHIFT:  "int",
}


def main(
		called_from_main: bool = False,
		partition_group: int = 1,
		partition_total: int = 1,
) -> None:
	set_error_file_origin(STATS)
	set_error_folder(FOLDER_ERROR)
	set_partition_group(partition_group)
	set_partition_total(partition_total)
	if not called_from_main:
		assert False, f"stats was called from another module. This functionality is not yet supported."
	else:
		with open(f"{STATS}{HYPHEN}{PARAMETERS}{JSON}") as json_file:
			json_data = json.load(json_file)
			bool_stitch_epa: bool = json_data[PARAM_STITCH_EPA]
			bool_stitch_trends: bool = json_data[PARAM_STITCH_TRENDS]
			bool_run_metrics_epa: bool = json_data[PARAM_METRICS_EPA]
			bool_aggregate_metrics_epa: bool = json_data[PARAM_AGGREGATE_METRICS_EPA]
			bool_run_metrics_trends: bool = json_data[PARAM_METRICS_TRENDS]
			bool_aggregate_metrics_trends: bool = json_data[PARAM_AGGREGATE_METRICS_TRENDS]
			bool_run_correlations: bool = json_data[PARAM_CORRELATE]
			bool_correlate_above_threshold: bool = json_data[PARAM_CORRELATE_ABOVE_THRESHOLD]
			bool_correlate_below_threshold: bool = json_data[PARAM_CORRELATE_BELOW_THRESHOLD]
			bool_only_correlate_missing: bool = json_data[PARAM_ONLY_CORRELATE_MISSING]
			bool_aggregate_correlations: bool = json_data[PARAM_AGGREGATE_CORRELATIONS]
			bool_aggregate_already_aggregated_cities: bool = json_data[PARAM_AGGREGATE_ALREADY_AGGREGATED_CITIES]
			bool_aggregate_and_upload: bool = json_data[PARAM_AGGREGATE_AND_UPLOAD]
			bool_run_intercity: bool = json_data[PARAM_INTERCITY]
			bool_aggregate_intercity: bool = json_data[PARAM_AGGREGATE_INTERCITY]
			str_upload_aggregate_from_folder: str = json_data[PARAM_UPLOAD_AGGREGATE_FROM_FOLDER]
			str_upload_folder: str = json_data[PARAM_UPLOAD_FOLDER]

			common_city: str = json_data[COMMON_CITY]
			common_word: str = json_data[COMMON_WORD]
			target_variable_column_name_epa: str = json_data[f"{TARGET_VARIABLE_COLUMN_NAME}{UNDERSCORE}{EPA}"]
			target_variable_column_name_trends: str = json_data[f"{TARGET_VARIABLE_COLUMN_NAME}{UNDERSCORE}{TRENDS}"]

			parameters: dict = json_data[STATS]
			start_date: str = parameters[START_DATE]
			end_date: str = parameters[END_DATE]
			list_input_cities: List[str] = parameters[CITY]
			list_pollutants: List[str] = parameters[POLLUTANT]
			list_target_statistics: List[str] = parameters[TARGET_STATISTIC]
			list_time_shifts: List[int] = parameters[TIME_SHIFT]
			DEFAULT_POLLUTANTS.update(parameters[POLLUTANT])

		json_file.close()

	list_threshold_sides: List[str] = []
	if bool_correlate_above_threshold:
		list_threshold_sides.append(CORRELATE_ABOVE_THRESHOLD)
	if bool_correlate_below_threshold:
		list_threshold_sides.append(CORRELATE_BELOW_THRESHOLD)

	city: str
	is_valid_for_aggregation: bool

	list_partitioned_cities: Tuple[str, ...] = tuple(
		partition_list(
			list_partition_candidates=list_input_cities,
			partition_group=get_partition_group(),
			partition_total=get_partition_total(),
		)
	)

	def mean_max_correlation():
		print("starting mean max")
		# running correlations for mean to max per site
		list_dict_mean_max_correlations = []
		for m_city in list_input_cities:
			list_site_numbers_per_city: List[str] = []
			for epa_filename in import_paths_from_folder(
					folder=FOLDER_EPA_STITCH,
					list_paths_filter_conditions=(m_city,),
			):
				nt_filename_epa_stitch = parse_filename(
					filename=epa_filename,
					delimiter=HYPHEN,
					named_tuple=epa.NT_filename_epa_stitch,
					extension=CSV,
				)
				if nt_filename_epa_stitch.target_statistic in [MEAN, MAX, ]:
					if nt_filename_epa_stitch.site_number == "~1" or nt_filename_epa_stitch.site_number in list_site_numbers_per_city:
						continue
					else:
						other_target_statistic: str
						if nt_filename_epa_stitch.target_statistic == MEAN:
							other_target_statistic = MAX
						elif nt_filename_epa_stitch.target_statistic == MAX:
							other_target_statistic = MEAN
						else:
							other_target_statistic = ""
							log_error(
								error=f"invalid_target_statistic{HYPHEN}{nt_filename_epa_stitch.target_statistic}")
							continue

						list_site_numbers_per_city.append(nt_filename_epa_stitch.site_number)
						list_other_matches = import_paths_from_folder(
							folder=FOLDER_EPA_STITCH,
							list_paths_filter_conditions=(
								m_city,
								nt_filename_epa_stitch.pollutant,
								other_target_statistic,
								nt_filename_epa_stitch.site_number,
							),
						)
						other_target_statistic_file = ""
						for match in list_other_matches:
							nt_filename_epa_match = parse_filename(
								filename=match,
								delimiter=HYPHEN,
								named_tuple=epa.NT_filename_epa_stitch,
								extension=CSV,
							)
							if nt_filename_epa_match.site_number == nt_filename_epa_stitch.site_number:
								if other_target_statistic_file:
									log_error(f"multiple_matches{HYPHEN}{other_target_statistic_file}")
								other_target_statistic_file = match

						df_one: pd.DataFrame = pd.read_csv(
							f"{FOLDER_EPA_STITCH}{epa_filename}",
							index_col=DATE,
							parse_dates=[DATE],
						)
						df_two: pd.DataFrame = pd.read_csv(
							f"{FOLDER_EPA_STITCH}{other_target_statistic_file}",
							index_col=DATE,
							parse_dates=[DATE],
						)
						pearson_correlation: float = df_one[target_variable_column_name_epa].corr(
							df_two[target_variable_column_name_epa],
							method="pearson",
						)
						dict_data = {}
						dict_data.update({
							CITY:                   m_city,
							EPA_COLUMN_SITE_NUMBER: nt_filename_epa_stitch.site_number,
							POLLUTANT:              nt_filename_epa_stitch.pollutant,
							PEARSON_CORRELATION: pearson_correlation,
						})
						list_dict_mean_max_correlations.append(dict_data)

				df = pd.DataFrame(list_dict_mean_max_correlations)
				print(df)
				nt_filename_mean_max_epa = NT_filename_mean_max_epa(
					city=m_city,
					pollutant=nt_filename_epa_stitch.pollutant,
					site_number=nt_filename_epa_stitch.site_number,
				)
				output_filename: str = generate_filename(
					nt_filename=nt_filename_mean_max_epa,
					extension=CSV,
					delimiter=HYPHEN,
				)
				generate_sub_paths_for_folder("../CGAP-data/CGAP-epa/correlations/{output_filename}")
				df.to_csv(f"../CGAP-data/CGAP-epa/correlations/{output_filename}")
		df_aggregate = pd.DataFrame(list_dict_mean_max_correlations)
		print(df_aggregate)
		df_aggregate.to_csv(f"../CGAP-data/CGAP-epa/aggregate-correlations-site-number.csv")

	mean_max_correlation()

	if bool_stitch_epa:
		print("Calling stitch epa from stats.")
		epa.main(
			called_from_main=False,
			list_cities=list_partitioned_cities,
			partition_group=get_partition_group(),
			partition_total=get_partition_total(),
		)

	if bool_stitch_trends:
		print("Calling stitch trends from stats.")
		trends.main(
			called_from_main=False,
			list_cities=list_partitioned_cities,
			partition_group=get_partition_group(),
			partition_total=get_partition_total(),
		)

	if bool_run_metrics_epa:
		set_error_task_origin(task_origin=PARAM_METRICS_EPA)
		for city in list_partitioned_cities:
			run_metrics(
				city=city,
				epa_or_trends=EPA,
				target_variable_column_name=target_variable_column_name_epa,
				ignore_zero=DEFAULT_IGNORE_ZERO_EPA,
				folder_metrics_input=FOLDER_EPA_STITCH,
				folder_metrics_output=FOLDER_EPA_METRICS_RAW,
			)
		write_errors_to_disk()

	if bool_aggregate_metrics_epa:
		set_error_task_origin(task_origin=PARAM_AGGREGATE_METRICS_EPA)
		is_valid_for_aggregation = check_partition_valid_for_aggregation(
			error_label=PARAM_AGGREGATE_METRICS_EPA,
			partition_group=get_partition_group(),
			partition_total=get_partition_total(),
		)
		if is_valid_for_aggregation:
			aggregate_data_in_folder(
				filename_label=PARAM_METRICS_EPA,
				folder_input=FOLDER_EPA_METRICS_RAW,
				folder_output_aggregate=FOLDER_EPA_METRICS_AGGREGATE,
				list_cities=list_partitioned_cities,
				upload=bool_aggregate_and_upload,
			)
		write_errors_to_disk()

	if bool_run_metrics_trends:
		set_error_task_origin(task_origin=PARAM_METRICS_TRENDS)
		for city in list_partitioned_cities:
			ignore_zero: bool
			for ignore_zero in DEFAULT_IGNORE_ZERO_TRENDS:
				run_metrics(
					city=city,
					epa_or_trends=TRENDS,
					target_variable_column_name=target_variable_column_name_trends,
					ignore_zero=ignore_zero,
					folder_metrics_input=FOLDER_TRENDS_STITCH,
					folder_metrics_output=FOLDER_TRENDS_METRICS_RAW,
				)
		write_errors_to_disk()

	if bool_aggregate_metrics_trends:
		set_error_task_origin(task_origin=PARAM_AGGREGATE_METRICS_TRENDS)
		is_valid_for_aggregation = check_partition_valid_for_aggregation(
			error_label=PARAM_AGGREGATE_METRICS_TRENDS,
			partition_group=get_partition_group(),
			partition_total=get_partition_total(),
		)
		if is_valid_for_aggregation:
			aggregate_data_in_folder(
				filename_label=PARAM_METRICS_TRENDS,
				folder_input=FOLDER_TRENDS_METRICS_RAW,
				folder_output_aggregate=FOLDER_TRENDS_METRICS_AGGREGATE,
				list_cities=list_partitioned_cities,
				upload=bool_aggregate_and_upload,
			)
		write_errors_to_disk()

	if bool_run_correlations:
		for city in list_partitioned_cities:
			run_correlations(
				city=city,
				folder_stats_correlations=FOLDER_STATS_CORRELATIONS_RAW,
				only_correlate_missing=bool_only_correlate_missing,
				start_date=start_date,
				end_date=end_date,
				target_variable_column_name_epa=target_variable_column_name_epa,
				target_variable_column_name_trends=target_variable_column_name_trends,
				folder_epa_stitch=FOLDER_EPA_STITCH,
				folder_trends_stitch=FOLDER_TRENDS_STITCH,
				list_bool_ignore_zero=DEFAULT_IGNORE_ZERO_TRENDS,
				list_pollutants=tuple(list_pollutants),
				list_target_statistics=tuple(list_target_statistics),
				list_threshold_sides=tuple(list_threshold_sides),
				list_time_shifts=tuple(list_time_shifts),
			)

	if bool_aggregate_correlations:
		set_error_task_origin(task_origin=PARAM_AGGREGATE_CORRELATIONS)
		is_valid_for_aggregation = check_partition_valid_for_aggregation(
			error_label=CORRELATIONS,
			partition_group=get_partition_group(),
			partition_total=get_partition_total(),
		)
		if is_valid_for_aggregation:
			if bool_aggregate_already_aggregated_cities:
				aggregate_data_in_folder(
					filename_label=CORRELATIONS,
					folder_input=FOLDER_STATS_CORRELATIONS_AGGREGATE,
					folder_output_aggregate=FOLDER_STATS_CORRELATIONS_AGGREGATE,
					list_cities=list_partitioned_cities,
					upload=bool_aggregate_and_upload,
				)
			else:
				aggregate_data_in_folder(
					filename_label=CORRELATIONS,
					folder_input=FOLDER_STATS_CORRELATIONS_RAW,
					folder_output_aggregate=FOLDER_STATS_CORRELATIONS_AGGREGATE,
					list_cities=list_partitioned_cities,
					upload=bool_aggregate_and_upload,
				)
		write_errors_to_disk(overwrite=(not bool_aggregate_already_aggregated_cities))

	if bool_run_intercity:
		set_error_task_origin(task_origin=PARAM_INTERCITY)
		is_valid_for_aggregation = check_partition_valid_for_aggregation(
			error_label=INTERCITY,
			partition_group=get_partition_group(),
			partition_total=get_partition_total(),
		)
		if is_valid_for_aggregation:
			run_intercity(
				common_city=common_city,
				common_word=common_word,
				list_input_cities=tuple(list_input_cities),
				folder_trends_stitch=FOLDER_TRENDS_STITCH,
				folder_stats_intercity=FOLDER_STATS_INTERCITY_RAW,
			)
		write_errors_to_disk(overwrite=False)

	if bool_aggregate_intercity:
		set_error_task_origin(task_origin=PARAM_AGGREGATE_INTERCITY)
		is_valid_for_aggregation = check_partition_valid_for_aggregation(
			error_label=INTERCITY,
			partition_group=get_partition_group(),
			partition_total=get_partition_total(),
		)
		if is_valid_for_aggregation:
			aggregate_data_in_folder(
				filename_label=INTERCITY,
				folder_input=FOLDER_STATS_INTERCITY_RAW,
				folder_output_aggregate=FOLDER_STATS_INTERCITY_AGGREGATE,
				list_cities=list_partitioned_cities,
				upload=bool_aggregate_and_upload,
			)
		write_errors_to_disk()

	if str_upload_aggregate_from_folder:
		upload_aggregate_from_folder(
			filename_label=str_upload_aggregate_from_folder.replace(f"{UNDERSCORE}{AGGREGATE}", ""),
			folder=universal_parameters[str_upload_aggregate_from_folder],
		)

	if str_upload_folder:
		upload_folder(
			filename_label=str_upload_folder.replace(f"{HYPHEN}{STITCH}", ""),
			folder=universal_parameters[str_upload_folder],
		)

def upload_folder(
		filename_label: str,
		folder: str,
) -> None:
	set_error_task_origin(task_origin=UPLOAD)
	if folder:
		upload_to_bigquery(
			path=folder,
			table_name=filename_label,
			file_or_folder=FOLDER,
		)
	write_errors_to_disk(overwrite=False)

def upload_aggregate_from_folder(
		filename_label: str,
		folder: str,
) -> None:
	set_error_task_origin(task_origin=UPLOAD)
	filename_upload = import_single_file(
		folder=folder,
		list_filename_filter_conditions=(AGGREGATE, CSV),
	)
	if filename_upload:
		upload_to_bigquery(
			path=f"{folder}{filename_upload}",
			table_name=filename_label,
			file_or_folder=FILE,
		)
	write_errors_to_disk(overwrite=False)

def run_metrics(
		city: str,
		epa_or_trends: str,
		target_variable_column_name: str,
		ignore_zero: bool,
		folder_metrics_input: str,
		folder_metrics_output: str,
) -> None:
	log_error(f"{METRICS} : {epa_or_trends} : {IGNORE_ZERO} {HYPHEN} {ignore_zero} : {city}", log=True)
	generate_sub_paths_for_folder(
		folder=folder_metrics_output,
	)

	filename: str
	for filename in import_paths_from_folder(
			folder=folder_metrics_input,
			list_paths_filter_conditions=(city, CSV),
	):
		try:
			df_full: pd.DataFrame = pd.read_csv(
				f"{folder_metrics_input}{filename}",
				parse_dates=[DATE],
			)
		except ValueError:
			log_error(error=f"value_error{HYPHEN}{filename}")
			continue
		if ignore_zero:
			df_full.replace(
				to_replace=0,
				value=np.nan,
				inplace=True,
			)

		df_full[YEAR] = pd.DatetimeIndex(df_full[DATE]).year
		list_years: list = list(df_full[YEAR].unique())
		list_years.append(0)
		for year in list_years:
			if year:
				df: pd.DataFrame = df_full[df_full[YEAR] == year]
			else:
				df = df_full

			count: int = df[target_variable_column_name].count()
			length_time_series: int = len(df[DATE].unique())
			df_description: pd.DataFrame = df[target_variable_column_name].describe()
			df_description = df_description.to_frame()
			df_description = df_description.transpose()
			df_description.insert(0, CITY, city)
			df_description.insert(1, IGNORE_ZERO, ignore_zero)
			df_description.insert(2, YEAR, year)
			df_description.insert(3, SPARSITY, (count / length_time_series))

			output_filename: str
			nt_filename_epa_or_trends_parsed: NamedTuple
			if epa_or_trends == EPA:
				# noinspection PyTypeChecker
				nt_filename_epa_or_trends_parsed: NamedTuple = parse_filename(
					filename=filename,
					delimiter=HYPHEN,
					extension=CSV,
					named_tuple=epa.NT_filename_epa_stitch,
				)
				df_description.insert(0, POLLUTANT, nt_filename_epa_or_trends_parsed.pollutant)
				df_description.insert(1, TARGET_STATISTIC, nt_filename_epa_or_trends_parsed.target_statistic)
				site_number: int = parse_filename_numeric(numeric=nt_filename_epa_or_trends_parsed.site_number,
														  cast_type="int")
				df_description.insert(
					2,
					EPA_COLUMN_SITE_NUMBER,
					site_number,
				)

				nt_output_filename = NT_filename_metrics_epa(
					city=city,
					pollutant=nt_filename_epa_or_trends_parsed.pollutant,
					site_number=generate_numeric_for_filename_output(site_number),
					target_statistic=nt_filename_epa_or_trends_parsed.target_statistic,
					ignore_zero=str(ignore_zero),
					year=str(year),
				)
			elif epa_or_trends == TRENDS:
				# noinspection PyTypeChecker
				nt_filename_epa_or_trends_parsed: NamedTuple = parse_filename(
					filename=filename,
					delimiter=HYPHEN,
					extension=CSV,
					named_tuple=trends.NT_filename_trends_stitch,
				)
				df_description.insert(0, KEYWORD, nt_filename_epa_or_trends_parsed.keyword)
				df_description.insert(1, COMMON_WORD, nt_filename_epa_or_trends_parsed.common_word)
				nt_output_filename = NT_filename_metrics_trends(
					city=city,
					common_word=nt_filename_epa_or_trends_parsed.common_word,
					keyword=nt_filename_epa_or_trends_parsed.keyword,
					ignore_zero=str(ignore_zero),
					year=str(year),
				)
			else:
				log_error(error=f"epa_or_trends{HYPHEN}{MISSING}")
				return

			if nt_filename_epa_or_trends_parsed.city != city:
				log_error(error=f"city_mismatch{HYPHEN}{city}{HYPHEN}{filename}")
				continue

			df_description.rename(
				columns={
					"25%": "quantile_25",
					"50%": "quantile_50",
					"75%": "quantile_75",
				},
				inplace=True,
			)
			output_filename = generate_filename(
				nt_filename=nt_output_filename,
				delimiter=HYPHEN,
				extension=CSV,
				folder=folder_metrics_output,
			)
			df_description.to_csv(
				output_filename,
				index=False,
			)

def generate_correlations_comparison_folder_name(
		folder_correlations_comparison: str,
		dict_correlations_comparison_pivot_values: Dict[str, Any],
) -> str:
	generate_sub_paths_for_folder(
		folder=folder_correlations_comparison,
	)
	list_correlations_comparison_folder_name_components: List[str] = [
		str(column_name) + UNDERSCORE + str(column_value)
		for column_name, column_value in dict_correlations_comparison_pivot_values.items()
		if column_value
	]
	subfolder: str = HYPHEN.join(list_correlations_comparison_folder_name_components)

	return f"{folder_correlations_comparison}{subfolder}/"

def run_correlations(
		city: str,
		folder_stats_correlations: str,
		only_correlate_missing: bool = DEFAULT_ONLY_CORRELATE_MISSING,
		start_date: str = "",
		end_date: str = "",
		target_variable_column_name_epa: str = POLLUTION_LEVEL,
		target_variable_column_name_trends: str = KEYWORD_FREQUENCY,
		folder_epa_stitch: str = FOLDER_EPA_STITCH,
		folder_trends_stitch: str = FOLDER_TRENDS_STITCH,
		list_bool_ignore_zero: Tuple[bool, ...] = DEFAULT_IGNORE_ZERO_TRENDS,
		list_pollutants: Tuple[str, ...] = tuple(DEFAULT_POLLUTANTS),
		list_target_statistics: Tuple[str, ...] = DEFAULT_TARGET_STATISTICS,
		list_threshold_sides: Tuple[str, ...] = DEFAULT_THRESHOLD_SIDES,
		list_time_shifts: Tuple[int, ...] = DEFAULT_TIME_SHIFTS,
) -> None:
	generate_sub_paths_for_folder(
		folder=folder_stats_correlations,
	)

	list_filenames_epa: List[str] = import_paths_from_folder(
		folder=folder_epa_stitch,
		list_paths_filter_conditions=(CSV,),
	)

	list_filenames_trends: List[str] = import_paths_from_folder(
		folder=folder_trends_stitch,
		list_paths_filter_conditions=(city, CSV),
	)

	list_filenames_correlations: List[str] = []
	if only_correlate_missing:
		list_filenames_correlations = import_paths_from_folder(
			folder=folder_stats_correlations,
			list_paths_filter_conditions=(city, CSV),
		)

	pollutant: str
	for pollutant in list_pollutants:
		dict_thresholds: Dict[float, str] = {
			threshold: threshold_source
			for threshold_source, threshold_list in DEFAULT_POLLUTANTS[pollutant][THRESHOLD].items()
			if (threshold_source in [CGAP, EPA]) or (city in threshold_source)
			for threshold in threshold_list
		}
		target_statistic: str
		for target_statistic in list_target_statistics:
			set_error_task_origin(task_origin=PARAM_CORRELATE)
			log_error(f"{CORRELATIONS} : {city} : {pollutant} : {target_statistic}", log=True)

			total_epa_days_count: int = -1

			list_potential_filename_epa: List[str] = filter_list_strings(
				list_strings=list_filenames_epa,
				list_string_filter_conditions=(city, pollutant, target_statistic, CSV),
			)
			filename_epa: str
			for filename_epa in list_potential_filename_epa:
				if not filename_epa:
					log_error(
						error=f"file_missing{HYPHEN}{EPA}{HYPHEN}{city}{HYPHEN}{pollutant}{HYPHEN}{target_statistic}")
					continue

				# noinspection PyTypeChecker
				nt_filename_epa_stitch_parsed: NamedTuple = parse_filename(
					filename=filename_epa,
					delimiter=HYPHEN,
					extension=CSV,
					named_tuple=epa.NT_filename_epa_stitch,
				)

				if nt_filename_epa_stitch_parsed.city != city:
					log_error(
						error=f"attribute_mismatch{HYPHEN}{EPA}{HYPHEN}{CITY}{HYPHEN}{nt_filename_epa_stitch_parsed.city}")
					log_error(error=f"{nt_filename_epa_stitch_parsed}")
					continue
				if nt_filename_epa_stitch_parsed.pollutant != pollutant:
					log_error(
						error=f"attribute_mismatch{HYPHEN}{EPA}{HYPHEN}{POLLUTANT}{HYPHEN}{nt_filename_epa_stitch_parsed.pollutant}")
					log_error(error=f"{nt_filename_epa_stitch_parsed}")
					continue
				if nt_filename_epa_stitch_parsed.target_statistic != target_statistic:
					log_error(
						error=f"attribute_mismatch{HYPHEN}{EPA}{HYPHEN}{TARGET_STATISTIC}{HYPHEN}{nt_filename_epa_stitch_parsed.target_statistic}")
					log_error(error=f"{nt_filename_epa_stitch_parsed}")
					continue

				site_number: int = parse_filename_numeric(
					numeric=nt_filename_epa_stitch_parsed.site_number,
					cast_type='int',
				)

				df_epa = pd.read_csv(
					f"{folder_epa_stitch}{filename_epa}",
					usecols=[
						DATE,
						target_variable_column_name_epa,
						SITE_COUNT,
					],
					parse_dates=[DATE],
				)
				if df_epa.empty:
					log_error(error=f"df_empty{HYPHEN}{EPA}{HYPHEN}{filename_epa}")

				if start_date and end_date:
					df_epa = filter_date_for_df(
						df=df_epa,
						date_column_is_index=False,
						date_column=DATE,
						start_date=start_date,
						end_date=end_date,
					)

				total_epa_days_count = df_epa[target_variable_column_name_epa].count()

				dict_epa_stats_helper: Dict[float, Dict[str, Dict[str, Any]]] = dp_epa_variations_dict(
					df_epa=df_epa,
					total_epa_days_count=total_epa_days_count,
					list_thresholds=tuple(dict_thresholds.keys()),
					list_threshold_sides=list_threshold_sides,
					target_variable_column_name_epa=target_variable_column_name_epa,
				)

				def correlate_trends() -> None:
					filename_trends: str
					for filename_trends in list_filenames_trends:
						df_trends: pd.DataFrame = pd.DataFrame()
						trends_set: bool = False

						# noinspection PyTypeChecker
						nt_filename_trends_stitch_parsed: NamedTuple = parse_filename(
							filename=filename_trends,
							delimiter=HYPHEN,
							extension=CSV,
							named_tuple=trends.NT_filename_trends_stitch,
						)

						if nt_filename_trends_stitch_parsed.city != city:
							log_error(error=f"attribute_mismatch{HYPHEN}{TRENDS}{HYPHEN}{CITY}{HYPHEN}{city}")
							continue

						def correlate_single_trend():
							nonlocal df_trends
							nonlocal trends_set

							kw_nonzero_count: int = -1
							kw_proportion: float = -1
							trends_column_name_ignore_zero: str = ""

							threshold: float
							for threshold in tuple(dict_thresholds.keys()):
								above_or_below_threshold: str
								for above_or_below_threshold in list_threshold_sides:
									bool_ignore_zero: bool
									for bool_ignore_zero in list_bool_ignore_zero:
										time_shift: int
										for time_shift in list_time_shifts:
											nt_filename_correlation = NT_filename_correlation(
												city=city,
												keyword=nt_filename_trends_stitch_parsed.keyword,
												pollutant=pollutant,
												site_number=nt_filename_epa_stitch_parsed.site_number,
												target_statistic=target_statistic,
												ignore_zero=str(bool_ignore_zero),
												threshold=generate_numeric_for_filename_output(threshold),
												threshold_side=above_or_below_threshold,
												time_shift=generate_numeric_for_filename_output(time_shift),
											)
											filename_correlation: str = generate_filename(
												nt_filename=nt_filename_correlation,
												delimiter=HYPHEN,
												extension=CSV,
											)

											if only_correlate_missing and filename_correlation in list_filenames_correlations:
												continue
											else:

												if not trends_set:
													df_trends = pd.read_csv(
														f"{folder_trends_stitch}{filename_trends}",
														usecols=[DATE, target_variable_column_name_trends],
														parse_dates=[DATE],
													)
													df_trends.set_index(DATE, inplace=True)
													trends_set = True
													if df_trends.empty:
														log_error(
															error=f"df_empty{HYPHEN}{TRENDS}{HYPHEN}{filename_trends}")
														return

													trends_column_name_ignore_zero: str = f"{target_variable_column_name_trends}{HYPHEN}{IGNORE_ZERO}"
													df_trends[trends_column_name_ignore_zero] = df_trends[
														target_variable_column_name_trends].replace(
														to_replace=0,
														value=np.nan,
													)

													kw_nonzero_count = df_trends[
														trends_column_name_ignore_zero].count()
													kw_proportion = kw_nonzero_count / df_trends[
														target_variable_column_name_trends].count()

												if bool_ignore_zero:
													trends_column = trends_column_name_ignore_zero
												else:
													trends_column = target_variable_column_name_trends

												df_epa_above_or_below_threshold: pd.DataFrame = \
													dict_epa_stats_helper[threshold][above_or_below_threshold][
														DATA_FRAME]
												threshold_epa_days_count: int = \
													dict_epa_stats_helper[threshold][above_or_below_threshold][
														THRESHOLD_EPA_DAYS_COUNT]
												threshold_epa_days_proportion: float = \
													dict_epa_stats_helper[threshold][above_or_below_threshold][
														THRESHOLD_EPA_DAYS_PROPORTION]
												threshold_site_count_avg: float = \
													dict_epa_stats_helper[threshold][above_or_below_threshold][
														THRESHOLD_SITE_COUNT_AVG]
												threshold_site_count_std: float = \
													dict_epa_stats_helper[threshold][above_or_below_threshold][
														THRESHOLD_SITE_COUNT_STD]

												dict_cor_row: dict = correlate_for_keyword(
													df_trends=df_trends,
													df_epa_above_or_below_threshold=df_epa_above_or_below_threshold,
													target_variable_column_name_epa=target_variable_column_name_epa,
													time_shift=time_shift,
													trends_column=trends_column,
													trends_column_name_ignore_zero=trends_column_name_ignore_zero,
													threshold_epa_days_count=threshold_epa_days_count,
												)

												dict_cor_row.update(
													{
														CITY:                          city,
														KEYWORD:                       nt_filename_trends_stitch_parsed.keyword,
														POLLUTANT:                     pollutant,
														EPA_COLUMN_SITE_NUMBER:        site_number,
														TARGET_STATISTIC:              target_statistic,
														THRESHOLD:                     threshold,
														THRESHOLD_SIDE:                above_or_below_threshold,
														THRESHOLD_SOURCE:              dict_thresholds.get(
															threshold, ""),
														TIME_SHIFT:                    time_shift,
														IGNORE_ZERO:                   bool_ignore_zero,
														TOTAL_EPA_DAYS_COUNT:          total_epa_days_count,
														THRESHOLD_EPA_DAYS_COUNT:      threshold_epa_days_count,
														THRESHOLD_EPA_DAYS_PROPORTION: threshold_epa_days_proportion,
														THRESHOLD_SITE_COUNT_AVG:      threshold_site_count_avg,
														THRESHOLD_SITE_COUNT_STD:      threshold_site_count_std,
														KW_NONZERO_COUNT:              kw_nonzero_count,
														KW_NONZERO_PROPORTION:         kw_proportion,
													},
												)

												pd.DataFrame(
													dict_cor_row,
													index=[0],
												).to_csv(
													f"{folder_stats_correlations}{filename_correlation}",
													index=False,
												)

						correlate_single_trend()

				correlate_trends()
			write_errors_to_disk(bool_suppress_print=True, overwrite=False)

# dynamic programming
def dp_epa_variations_dict(
		df_epa: pd.DataFrame,
		total_epa_days_count: int,
		list_thresholds: Tuple[float, ...],
		list_threshold_sides: Tuple[str, ...] = DEFAULT_THRESHOLD_SIDES,
		target_variable_column_name_epa: str = POLLUTION_LEVEL,
) -> Dict[float, Dict[str, Dict[str, Any]]]:
	dict_epa_stats_helper: Dict[float, Dict[str, Dict[str, Any]]] = {}
	for threshold in list_thresholds:
		dict_epa_stats_helper.update({threshold: {}})
		for threshold_side in list_threshold_sides:
			dict_epa_stats_helper[threshold].update({threshold_side: {}})
			if threshold_side == CORRELATE_ABOVE_THRESHOLD:
				df_epa_above_or_below_threshold: pd.DataFrame = df_epa.mask(
					df_epa[target_variable_column_name_epa] < threshold)
			elif threshold_side == CORRELATE_BELOW_THRESHOLD:
				df_epa_above_or_below_threshold: pd.DataFrame = df_epa.mask(
					df_epa[target_variable_column_name_epa] >= threshold)
			else:
				continue

			threshold_site_count_avg = df_epa_above_or_below_threshold[SITE_COUNT].mean()
			threshold_site_count_std = df_epa_above_or_below_threshold[SITE_COUNT].std()

			threshold_epa_days_count: int = df_epa_above_or_below_threshold[target_variable_column_name_epa].count()
			threshold_epa_days_proportion: float
			if total_epa_days_count > 0 and threshold_epa_days_count > 0:
				threshold_epa_days_proportion = threshold_epa_days_count / total_epa_days_count
			else:
				threshold_epa_days_proportion = -1

			dict_epa_stats_helper[threshold][threshold_side].update({
				DATA_FRAME:                    df_epa_above_or_below_threshold,
				THRESHOLD_EPA_DAYS_COUNT:      threshold_epa_days_count,
				THRESHOLD_EPA_DAYS_PROPORTION: threshold_epa_days_proportion,
				THRESHOLD_SITE_COUNT_AVG:      threshold_site_count_avg,
				THRESHOLD_SITE_COUNT_STD:      threshold_site_count_std,
			})

	return dict_epa_stats_helper

def correlate_for_keyword(
		df_trends: pd.DataFrame,
		df_epa_above_or_below_threshold: pd.DataFrame,
		target_variable_column_name_epa: str,
		time_shift: int,
		trends_column: str,
		trends_column_name_ignore_zero: str,
		threshold_epa_days_count: int,
) -> dict:
	dict_cor_row: dict = {}

	# noinspection PyTypeChecker
	df_kw_nonzero_threshold_days: pd.DataFrame = (df_trends[trends_column_name_ignore_zero] > 0) & \
												 df_epa_above_or_below_threshold[target_variable_column_name_epa]
	kw_nonzero_threshold_days_count: int = df_kw_nonzero_threshold_days.count()
	dict_cor_row.update({KW_NON_ZERO_THRESHOLD_DAYS_COUNT: kw_nonzero_threshold_days_count})

	kw_threshold_site_count_avg = df_epa_above_or_below_threshold[SITE_COUNT].mean()
	dict_cor_row.update({KW_SITE_COUNT_AVG: kw_threshold_site_count_avg})
	kw_threshold_site_count_std = df_epa_above_or_below_threshold[SITE_COUNT].std()
	dict_cor_row.update({KW_SITE_COUNT_STD: kw_threshold_site_count_std})

	kw_non_zero_threshold_days_proportion: float
	if threshold_epa_days_count > 0:
		kw_non_zero_threshold_days_proportion = kw_nonzero_threshold_days_count / threshold_epa_days_count
	else:
		kw_non_zero_threshold_days_proportion = np.nan
	dict_cor_row.update({KW_NON_ZERO_THRESHOLD_DAYS_PROPORTION: kw_non_zero_threshold_days_proportion})

	# need to multiply time shift by -1 to keep intuitive logic
	# shift of 1 is tomorrow's search correlating with today's pollution
	df_trends_with_time_shift: pd.DataFrame = df_trends[trends_column].shift(periods=(-1 * time_shift))

	# noinspection PyArgumentList
	# noinspection PyTypeChecker
	pearson_correlation: float = df_trends_with_time_shift.corr(
		df_epa_above_or_below_threshold[target_variable_column_name_epa],
		method="pearson",
	)
	dict_cor_row.update({PEARSON_CORRELATION: pearson_correlation})

	# noinspection PyArgumentList
	# noinspection PyTypeChecker
	spearman_correlation: float = df_trends_with_time_shift.corr(
		df_epa_above_or_below_threshold[target_variable_column_name_epa],
		method="spearman",
	)
	dict_cor_row.update({SPEARMAN_CORRELATION: spearman_correlation})

	return dict_cor_row

def run_intercity(
		common_city: str = DEFAULT_COMMON_CITY,
		common_word: str = DEFAULT_COMMON_WORD,
		list_input_cities: Tuple[str, ...] = tuple(DEFAULT_CITIES),
		folder_trends_stitch: str = FOLDER_TRENDS_STITCH,
		folder_stats_intercity: str = FOLDER_STATS_INTERCITY_RAW,
) -> None:
	log_error(f"{INTERCITY}", log=True)
	generate_sub_paths_for_folder(
		folder=folder_stats_intercity,
	)
	city: str
	list_cities: List[str] = [
		city
		for city in list_input_cities
		if city != common_city
	]

	# todo - why is this partitioned based on words and not the city
	list_common_city_filenames: List[str] = import_paths_from_folder(
		folder=folder_trends_stitch,
		list_paths_filter_conditions=(common_city, common_word, CSV),
	)
	list_common_city_filenames = partition_list(
		list_partition_candidates=list_common_city_filenames,
		partition_group=get_partition_group(),
		partition_total=get_partition_total(),
	)

	common_city_filename: str
	for common_city_filename in list_common_city_filenames:
		# noinspection PyTypeChecker
		nt_filename_trends_stitch_parsed: NamedTuple = parse_filename(
			filename=common_city_filename,
			delimiter=HYPHEN,
			extension=CSV,
			named_tuple=trends.NT_filename_trends_stitch,
		)

		if nt_filename_trends_stitch_parsed.city != common_city:
			log_error(
				error=f"attribute_mismatch{HYPHEN}{CITY}{HYPHEN}{common_city}{HYPHEN}{nt_filename_trends_stitch_parsed.city}")
			continue
		if nt_filename_trends_stitch_parsed.common_word != common_word:
			log_error(
				error=f"attribute_mismatch{HYPHEN}{COMMON_WORD}{HYPHEN}{common_word}{HYPHEN}{nt_filename_trends_stitch_parsed.common_word}")
			continue

		print(f"{INTERCITY} : {nt_filename_trends_stitch_parsed.keyword}")

		df_common_city_keyword: pd.DataFrame = pd.read_csv(
			f"{folder_trends_stitch}{common_city_filename}",
			index_col=DATE,
			parse_dates=True,
			infer_datetime_format=True,
		)

		for city in list_cities:
			nt_filename_trends_stitch = trends.NT_filename_trends_stitch(
				city=city,
				keyword=nt_filename_trends_stitch_parsed.keyword,
				common_word=common_word,
				start_date=generate_date_for_filename_output(
					date=nt_filename_trends_stitch_parsed.start_date,
				),
				end_date=generate_date_for_filename_output(
					date=nt_filename_trends_stitch_parsed.end_date,
				),
			)
			filename_trends_stitch: str = generate_filename(
				nt_filename=nt_filename_trends_stitch,
				delimiter=HYPHEN,
				extension=CSV,
			)
			if filename_trends_stitch in import_paths_from_folder(
					folder=folder_trends_stitch,
					list_paths_filter_conditions=(city, common_word, CSV),
			):
				df_city_to_be_scaled: pd.DataFrame = pd.read_csv(
					f"{folder_trends_stitch}{filename_trends_stitch}",
					index_col=DATE,
					parse_dates=True,
					infer_datetime_format=True,
				)

				df_intercity: pd.DataFrame = pd.DataFrame(index=df_common_city_keyword.index)

				df_common_word_cross_city_scalar: pd.DataFrame = df_common_city_keyword[COMMON_WORD_FREQUENCY] / \
																 df_city_to_be_scaled[COMMON_WORD_FREQUENCY]
				df_keyword_frequency_scaled_within_city: pd.DataFrame = df_city_to_be_scaled[
																			COMMON_WORD_FREQUENCY] / \
																		df_city_to_be_scaled[
																			COMMON_WORD_FREQUENCY_RELATIVE] * \
																		df_city_to_be_scaled[
																			KEYWORD_FREQUENCY_RELATIVE]
				df_keyword_frequency_scaled_cross_city: pd.DataFrame = df_common_word_cross_city_scalar * df_keyword_frequency_scaled_within_city

				df_intercity[KEYWORD_FREQUENCY_RELATIVE] = df_keyword_frequency_scaled_cross_city
				df_intercity.insert(0, COMMON_WORD_FREQUENCY, df_common_city_keyword[COMMON_WORD_FREQUENCY])
				df_intercity.replace(
					to_replace=[np.inf, -np.inf],
					value=np.nan,
					inplace=True,
				)
				df_intercity.insert(0, COMMON_CITY, common_city)
				df_intercity.insert(1, COMMON_WORD, common_word)
				df_intercity.insert(2, KEYWORD, nt_filename_trends_stitch_parsed.keyword)
				df_intercity.insert(3, CITY, city)

				nt_filename_intercity: tuple = NT_filename_intercity(
					common_city=common_city,
					common_word=common_word,
					keyword=nt_filename_trends_stitch_parsed,
					city=city,
				)
				filename_intercity: str = generate_filename(
					nt_filename=nt_filename_intercity,
					delimiter=HYPHEN,
					extension=CSV,
					folder=folder_stats_intercity,
				)
				df_intercity.to_csv(
					filename_intercity,
					index=True,
				)
			else:
				log_error(error=f"missing_common_word_file{HYPHEN}{filename_trends_stitch}")

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
