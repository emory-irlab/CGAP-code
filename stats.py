import sys

import epa
import trends
from universal import *

# STATIC VARIABLES
COMMON_CITY: str = "common_city"
CORRELATIONS: str = "correlations"
CORRELATIONS_COMPARISON: str = "correlations_comparison"
CORRELATE_ABOVE_THRESHOLD: str = "above_threshold"
CORRELATE_BELOW_THRESHOLD: str = "below_threshold"
INTERCITY: str = "intercity"
KW_NONZERO_COUNT: str = "kw_nonzero_count"
KW_NONZERO_PROPORTION: str = "kw_nonzero_proportion"
KW_NONZERO_FOR_NON_POLLUTED_DAYS_COUNT: str = "kw_nonzero_for_non_polluted_days_count"
KW_NON_POLLUTED_DAYS_PROPORTION: str = "kw_nonzero_for_non_polluted_days_proportion"
METRICS: str = "metrics"
PEARSON_CORRELATION: str = "pearson_correlation"
NON_POLLUTED_DAYS_COUNT: str = "non_polluted_days_count"
NON_POLLUTED_DAYS_PROPORTION: str = "non_polluted_days_proportion"
SPARSITY: str = "sparsity"
SPEARMAN_CORRELATION: str = "spearman_correlation"
STATS: str = "stats"
TARGET_VARIABLE_COLUMN_NAME: str = "target_variable_column_name"

# PARAMETERS
PARAM_AGGREGATE_CORRELATIONS: str = "aggregate_correlations"
PARAM_AGGREGATE_INTERCITY: str = "aggregate_intercity"
PARAM_AGGREGATE_METRICS: str = "aggregate_metrics"
PARAM_AGGREGATE_METRICS_EPA: str = f"{PARAM_AGGREGATE_METRICS}{UNDERSCORE}{EPA}"
PARAM_AGGREGATE_METRICS_TRENDS = f"{PARAM_AGGREGATE_METRICS}{UNDERSCORE}{TRENDS}"
PARAM_AGGREGATE_ALREADY_AGGREGATED_CITIES: str = "aggregate_already_aggregated_cities"
PARAM_BASELINE: str = "baseline"
PARAM_CORRELATE: str = "correlate"
PARAM_CORRELATE_ABOVE_THRESHOLD: str = "correlate_above_threshold"
PARAM_CORRELATE_BELOW_THRESHOLD: str = "correlate_below_threshold"
PARAM_CORRELATIONS_COMPARISON_PIVOT_VALUES: str = "correlations_comparison_pivot_values"
PARAM_INTERCITY: str = "run_intercity"
PARAM_ONLY_BASELINE_MISSING: str = "only_baseline_missing"
PARAM_ONLY_CORRELATE_MISSING: str = "only_correlate_missing"
PARAM_METRICS: str = "run_metrics"
PARAM_METRICS_EPA: str = f"{PARAM_METRICS}{UNDERSCORE}{EPA}"
PARAM_METRICS_TRENDS: str = f"{PARAM_METRICS}{UNDERSCORE}{TRENDS}"
PARAM_STITCH_EPA: str = f"{STITCH}{UNDERSCORE}{EPA}"
PARAM_STITCH_TRENDS: str = f"{STITCH}{UNDERSCORE}{TRENDS}"

# DEFAULT
DEFAULT_COMMON_CITY: str = USA
DEFAULT_IGNORE_ZERO_EPA: bool = False
DEFAULT_ONLY_CORRELATE_MISSING: bool = True
DEFAULT_ONLY_COMPARE_MISSING: bool = True
DEFAULT_SPLIT_STATS_CORRELATION_FILENAME_LENGTH: int = 9
DEFAULT_IGNORE_ZERO_TRENDS: Tuple[bool, ...] = (
	True,
	False,
)

DEFAULT_THRESHOLD_PERCENTILES: Tuple[int, ...] = (
	100,
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

NT_filename_metrics_epa = namedtuple(
	"NT_filename_metrics_epa",
	[
		CITY,
		POLLUTANT,
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
			bool_run_correlations_comparison: bool = json_data[PARAM_BASELINE]
			bool_only_compare_missing: bool = json_data[PARAM_ONLY_BASELINE_MISSING]
			bool_aggregate_correlations: bool = json_data[PARAM_AGGREGATE_CORRELATIONS]
			bool_aggregate_already_aggregated_cities: bool = json_data[PARAM_AGGREGATE_ALREADY_AGGREGATED_CITIES]
			bool_run_intercity: bool = json_data[PARAM_INTERCITY]
			bool_aggregate_intercity: bool = json_data[PARAM_AGGREGATE_INTERCITY]

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
			list_threshold_percentiles: List[int] = parameters[THRESHOLD_PERCENTILE]
			list_time_shifts: List[int] = parameters[TIME_SHIFT]
			DEFAULT_POLLUTANTS.update(parameters[POLLUTANT])

			dict_correlations_comparison_pivot_values: Dict[str, Any] = json_data[
				PARAM_CORRELATIONS_COMPARISON_PIVOT_VALUES
			]

			folder_stats_correlations_raw: str = json_data[PARAM_FOLDER_STATS_CORRELATIONS_RAW]
			folder_stats_correlations_aggregate: str = json_data[PARAM_FOLDER_STATS_CORRELATIONS_AGGREGATE]
			folder_stats_correlations_comparison: str = json_data[PARAM_FOLDER_STATS_CORRELATIONS_COMPARISON]
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
		del city
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
				folder_input=FOLDER_EPA_METRICS_RAW,
				folder_output_aggregate=FOLDER_EPA_METRICS_AGGREGATE,
				list_cities=list_partitioned_cities,
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
		del city
		del ignore_zero
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
				folder_input=FOLDER_TRENDS_METRICS_RAW,
				folder_output_aggregate=FOLDER_TRENDS_METRICS_AGGREGATE,
				list_cities=list_partitioned_cities,
			)
		write_errors_to_disk()

	if bool_run_correlations:
		for city in list_partitioned_cities:
			run_correlations(
				city=city,
				folder_stats_correlations=folder_stats_correlations_raw,
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
				list_threshold_percentiles=tuple(list_threshold_percentiles),
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
					folder_input=folder_stats_correlations_aggregate,
					folder_output_aggregate=folder_stats_correlations_aggregate,
					list_cities=list_partitioned_cities,
				)
			else:
				aggregate_data_in_folder(
					folder_input=folder_stats_correlations_raw,
					folder_output_aggregate=folder_stats_correlations_aggregate,
					list_cities=list_partitioned_cities,
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
				folder_input=FOLDER_STATS_INTERCITY_RAW,
				folder_output_aggregate=FOLDER_STATS_INTERCITY_AGGREGATE,
				list_cities=list_partitioned_cities,
			)
		write_errors_to_disk()

	if bool_run_correlations_comparison:
		for city in list_partitioned_cities:
			set_error_task_origin(task_origin=PARAM_BASELINE)
			log_error(f"{CORRELATIONS_COMPARISON} : {city}", log=True)
			baseline(
				city=city,
				dict_pivot_values=dict_correlations_comparison_pivot_values,
				folder_stats_correlations=folder_stats_correlations_raw,
				folder_stats_correlations_comparison=folder_stats_correlations_comparison,
				only_compare_missing=bool_only_compare_missing,
				list_bool_ignore_zero=DEFAULT_IGNORE_ZERO_TRENDS,
				list_pollutants=tuple(list_pollutants),
				list_target_statistics=tuple(list_target_statistics),
				list_threshold_percentiles=tuple(list_threshold_percentiles),
				list_threshold_sides=tuple(list_threshold_sides),
				list_time_shifts=tuple(list_time_shifts),
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
	log_error(f"{METRICS} : {epa_or_trends} : {city} : {IGNORE_ZERO}{HYPHEN}{ignore_zero}", log=True)
	generate_sub_paths_for_folder(
		folder=folder_metrics_output,
	)

	filename: str
	for filename in import_paths_from_folder(
			folder=folder_metrics_input,
			list_paths_filter_conditions=(city, CSV),
	):
		df_full: pd.DataFrame = pd.read_csv(
			f"{folder_metrics_input}{filename}",
			parse_dates=[DATE],
		)
		if ignore_zero:
			df_full.replace(
				to_replace=0,
				value=np.NaN,
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

				nt_output_filename = NT_filename_metrics_epa(
					city=city,
					pollutant=nt_filename_epa_or_trends_parsed.pollutant,
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
			output_filename = generate_filename(
				nt_filename=nt_output_filename,
				delimiter=HYPHEN,
				extension=CSV,
			)
			df_description.to_csv(
				f"{folder_metrics_output}{output_filename}",
				index=False,
			)


def baseline(
		city: str,
		dict_pivot_values: Dict[str, Any],
		folder_stats_correlations: str,
		folder_stats_correlations_comparison: str,
		only_compare_missing: bool = DEFAULT_ONLY_COMPARE_MISSING,
		list_bool_ignore_zero: Tuple[bool, ...] = DEFAULT_IGNORE_ZERO_TRENDS,
		list_pollutants: Tuple[str, ...] = tuple(DEFAULT_POLLUTANTS),
		list_target_statistics: Tuple[str, ...] = DEFAULT_TARGET_STATISTICS,
		list_threshold_percentiles: Tuple[int, ...] = DEFAULT_THRESHOLD_PERCENTILES,
		list_threshold_sides: Tuple[str, ...] = DEFAULT_THRESHOLD_SIDES,
		list_time_shifts: Tuple[int, ...] = DEFAULT_TIME_SHIFTS,
) -> None:
	dict_comparison_constraints: Dict[str, Tuple[any, ...]] = {
		IGNORE_ZERO:          list_bool_ignore_zero,
		POLLUTANT:            list_pollutants,
		TARGET_STATISTIC:     list_target_statistics,
		THRESHOLD_PERCENTILE: list_threshold_percentiles,
		THRESHOLD_SIDE:       list_threshold_sides,
		TIME_SHIFT:           list_time_shifts,
	}
	list_filenames_correlations: List[str] = import_paths_from_folder(
		folder=folder_stats_correlations,
		list_paths_filter_conditions=(city, CSV),
	)

	folder_comparison_tables: str = generate_correlations_comparison_folder_name(
		folder_correlations_comparison=folder_stats_correlations_comparison,
		dict_correlations_comparison_pivot_values=dict_pivot_values,
	)
	list_comparison_tables_filenames: List[str] = import_paths_from_folder(
		folder=folder_comparison_tables,
		list_paths_filter_conditions=(city, CSV),
	)

	list_null_pivot_keys: List[str, Any] = [
		key
		for (key, value) in dict_pivot_values.items()
		if not value
	]

	dict_pivot_table_info_non_null: Dict[str, Any] = {
		key: value
		for (key, value) in dict_pivot_values.items()
		if value
	}

	filename_trends: str
	for filename_correlation in list_filenames_correlations:
		dict_pivot_table_info: Dict[str, Any] = parse_filename_correlations(filename_correlation)
		is_pivot_table: bool = all(
			[
				dict_pivot_table_info.get(pivot_column, None) == pivot_value
				for pivot_column, pivot_value in dict_pivot_table_info_non_null.items()
			]
		)
		is_pivot_table_in_constraints: bool = all(
			[
				dict_pivot_table_info.get(constraint_key, None) in constraint_value
				for constraint_key, constraint_value in dict_comparison_constraints.items()
			]
		)
		if is_pivot_table and is_pivot_table_in_constraints:
			pivot_table_set: bool = False
			df_pivot_table: pd.DataFrame = pd.DataFrame()

			list_pivot_table_filename_filter_conditions: Tuple[str, ...] = tuple(
				[
					str(dict_pivot_table_info.get(null_pivot_key, ""))
					for null_pivot_key in list_null_pivot_keys
				]
			)

			filename_comparison_table: str
			for filename_comparison_table in filter_list_strings(
					list_strings=list_filenames_correlations,
					list_string_filter_conditions=list_pivot_table_filename_filter_conditions,
			):
				if only_compare_missing and filename_comparison_table in list_comparison_tables_filenames:
					continue

				dict_comparison_table_info_stripped: Dict[str, Any] = {
					key: value
					for key, value in parse_filename_correlations(filename_comparison_table).items()
					if key in list_null_pivot_keys
				}
				dict_pivot_table_info_stripped: Dict[str, Any] = {
					key: value
					for key, value in dict_pivot_table_info
					if key in list_null_pivot_keys
				}
				if dict_comparison_table_info_stripped == dict_pivot_table_info_stripped:

					if not pivot_table_set:
						df_pivot_table = pd.read_csv(
							f"{folder_stats_correlations}{filename_correlation}",
						)
						pivot_table_set = True
					else:
						if df_pivot_table.empty:
							log_error(error=f"df_empty_despite_being_set{HYPHEN}{CORRELATIONS_COMPARISON}{HYPHEN}{filename_correlation}")
							continue

						df_comparison_table: pd.DataFrame = pd.read_csv(
							f"{folder_stats_correlations}{filename_comparison_table}",
						)

						df_correlation_comparison: pd.DataFrame = df_pivot_table - df_comparison_table
						df_correlation_comparison.to_csv(
							f"{folder_comparison_tables}{filename_comparison_table}"
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
		list_threshold_percentiles: Tuple[int, ...] = DEFAULT_THRESHOLD_PERCENTILES,
		list_threshold_sides: Tuple[str, ...] = DEFAULT_THRESHOLD_SIDES,
		list_time_shifts: Tuple[int, ...] = DEFAULT_TIME_SHIFTS,
) -> None:
	generate_sub_paths_for_folder(
		folder=folder_stats_correlations,
	)
	list_filenames_trends: List[str] = import_paths_from_folder(
		folder=folder_trends_stitch,
		list_paths_filter_conditions=(city, CSV),
	)

	list_filenames_correlations: List[str] = import_paths_from_folder(
		folder=folder_stats_correlations,
		list_paths_filter_conditions=(city, CSV),
	)

	pollutant: str
	for pollutant in list_pollutants:
		target_statistic: str
		for target_statistic in list_target_statistics:
			set_error_task_origin(task_origin=PARAM_CORRELATE)
			log_error(f"{CORRELATIONS} : {city} : {pollutant} : {target_statistic}", log=True)

			filename_trends: str
			for filename_trends in list_filenames_trends:
				df_trends: pd.DataFrame = pd.DataFrame()
				trends_set: bool = False
				df_epa: pd.DataFrame = pd.DataFrame()
				epa_set: bool = False

				# noinspection PyTypeChecker
				nt_filename_trends_stitch_parsed: NamedTuple = parse_filename(
					filename=filename_trends,
					delimiter=HYPHEN,
					extension=CSV,
					named_tuple=trends.NT_filename_trends_stitch,
				)

				if nt_filename_trends_stitch_parsed.city != city:
					continue

				threshold_percentile: int
				for threshold_percentile in list_threshold_percentiles:
					threshold: float
					for threshold in DEFAULT_POLLUTANTS[pollutant][THRESHOLD]:
						threshold = threshold * (threshold_percentile / 100)
						time_shift: int
						for time_shift in list_time_shifts:
							bool_ignore_zero: bool
							for bool_ignore_zero in list_bool_ignore_zero:
								above_or_below_threshold: str
								for above_or_below_threshold in list_threshold_sides:
									filename_correlation: str = generate_stats_correlations_filename(
										city=nt_filename_trends_stitch_parsed.city,
										keyword=nt_filename_trends_stitch_parsed.keyword,
										pollutant=pollutant,
										target_statistic=target_statistic,
										bool_ignore_zero=bool_ignore_zero,
										threshold=threshold,
										threshold_percentile=threshold_percentile,
										above_or_below_threshold=above_or_below_threshold,
										time_shift=time_shift,
									)

									if only_correlate_missing and filename_correlation in list_filenames_correlations:
										continue
									else:
										if not trends_set:
											df_trends = pd.read_csv(
												f"{folder_trends_stitch}{filename_trends}",
												parse_dates=[DATE],
											)
											trends_set = True
										else:
											if df_trends.empty:
												log_error(error=f"df_empty_despite_being_set{HYPHEN}{TRENDS}{HYPHEN}{filename_trends}")
												continue

										filename_epa: str = import_single_file(
											folder=folder_epa_stitch,
											list_filename_filter_conditions=(city, pollutant, target_statistic, CSV)
										)
										if not epa_set:
											if filename_epa:
												df_epa = pd.read_csv(
													f"{folder_epa_stitch}{filename_epa}",
													parse_dates=[DATE],
												)
												if start_date and end_date:
													df_epa = filter_date_for_df(
														df=df_epa,
														date_column_is_index=False,
														date_column=DATE,
														start_date=start_date,
														end_date=end_date,
													)
												epa_set = True
												if df_epa.empty:
													log_error(error=f"epa_df_empty{HYPHEN}{EPA}{HYPHEN}{filename_epa}")
													continue
											else:
												log_error(error=f"epa_file_missing{HYPHEN}{city}{HYPHEN}{pollutant}{HYPHEN}{target_statistic}")
												continue

										dict_cor_row: dict = correlate_for_keyword(
											df_epa=df_epa,
											target_variable_column_name_epa=target_variable_column_name_epa,
											df_trends=df_trends,
											target_variable_column_name_trends=target_variable_column_name_trends,
											threshold=threshold,
											above_or_below_threshold=above_or_below_threshold,
											time_shift=time_shift,
											bool_ignore_zero=bool_ignore_zero,
										)
										dict_cor_row.update(
											{
												CITY:                 city,
												KEYWORD:              nt_filename_trends_stitch_parsed.keyword,
												POLLUTANT:            pollutant,
												TARGET_STATISTIC:     target_statistic,
												THRESHOLD:            threshold,
												THRESHOLD_SIDE:       above_or_below_threshold,
												THRESHOLD_PERCENTILE: threshold_percentile,
												TIME_SHIFT:           time_shift,
											},
										)

										pd.DataFrame(
											dict_cor_row,
											index=[0],
										).to_csv(
											f"{folder_stats_correlations}{filename_correlation}",
											index=False,
										)
			write_errors_to_disk(overwrite=False)


# todo - switch to named tuple
def generate_stats_correlations_filename(
		city: str,
		keyword: str,
		pollutant: str,
		target_statistic: str,
		bool_ignore_zero: bool,
		threshold: float,
		threshold_percentile: int,
		above_or_below_threshold: str,
		time_shift: int,
) -> str:
	# @formatter:off
	filename: str = f"{city}{HYPHEN}" \
					f"{keyword}{HYPHEN}" \
					f"{pollutant}{HYPHEN}" \
					f"{target_statistic}{HYPHEN}" \
					f"{bool_ignore_zero}{HYPHEN}" \
					f"{generate_numeric_for_filename_output(threshold)}{HYPHEN}" \
					f"{threshold_percentile}{HYPHEN}" \
					f"{above_or_below_threshold}{HYPHEN}" \
					f"{generate_numeric_for_filename_output(time_shift)}" \
					f"{CSV}"
	# @formatter:on

	return filename


def parse_filename_correlations(
		filename: str,
) -> Dict[str, Any]:
	split_filename: List[str] = filename.replace(CSV, EMPTY_STRING).split(HYPHEN)
	split_filename_length = len(split_filename)
	if split_filename_length != DEFAULT_SPLIT_STATS_CORRELATION_FILENAME_LENGTH:
		log_error(error=f"file_incorrectly_formatted{HYPHEN}{filename}")

	dict_filename_correlations_parsed: dict = {
		CITY:                 split_filename[0],
		KEYWORD:              split_filename[1],
		POLLUTANT:            split_filename[2],
		TARGET_STATISTIC:     split_filename[3],
		IGNORE_ZERO:          bool(split_filename[4]),
		THRESHOLD:            float(parse_filename_numeric(split_filename[5])),
		THRESHOLD_PERCENTILE: int(split_filename[6]),
		THRESHOLD_SIDE:       split_filename[7],
		TIME_SHIFT:           int(parse_filename_numeric(split_filename[8]))
	}

	return dict_filename_correlations_parsed


def correlate_for_keyword(
		df_epa: pd.DataFrame,
		df_trends: pd.DataFrame,
		target_variable_column_name_epa: str,
		target_variable_column_name_trends: str,
		threshold: float,
		above_or_below_threshold: str,
		time_shift: int,
		bool_ignore_zero: bool,
) -> dict:
	dict_cor_row: dict = {}
	df_merged: pd.DataFrame = df_trends.merge(
		right=df_epa,
		left_on=DATE,
		right_on=DATE,
		how="left",
	)
	del df_epa
	del df_trends

	trends_column_name_ignore_zero: str = f"{target_variable_column_name_trends}{HYPHEN}{IGNORE_ZERO}"
	df_merged[trends_column_name_ignore_zero] = df_merged[target_variable_column_name_trends].replace(
		to_replace=0,
		value=np.NaN,
	)
	df_length: int = len(df_merged.index)

	kw_count: int = df_merged[trends_column_name_ignore_zero].count()
	dict_cor_row.update({KW_NONZERO_COUNT: kw_count})

	kw_proportion: float = kw_count / df_length
	dict_cor_row.update({KW_NONZERO_PROPORTION: kw_proportion})

	if threshold >= 0:
		df_non_polluted: pd.DataFrame = df_merged[target_variable_column_name_epa] < threshold
		non_polluted_days_count: int = df_non_polluted.sum()
		dict_cor_row.update({NON_POLLUTED_DAYS_COUNT: non_polluted_days_count})

		non_polluted_days_proportion: float = non_polluted_days_count / df_length
		dict_cor_row.update({NON_POLLUTED_DAYS_PROPORTION: non_polluted_days_proportion})

		df_kw_non_polluted_days: pd.DataFrame = df_merged[
			((df_merged[trends_column_name_ignore_zero] > 0) & (df_merged[target_variable_column_name_epa] < threshold))
		]
		kw_non_polluted_days_count: int = len(df_kw_non_polluted_days.index)
		dict_cor_row.update({KW_NONZERO_FOR_NON_POLLUTED_DAYS_COUNT: kw_non_polluted_days_count})

		kw_non_polluted_days_proportion: float = kw_non_polluted_days_count / non_polluted_days_count
		dict_cor_row.update({KW_NON_POLLUTED_DAYS_PROPORTION: kw_non_polluted_days_proportion})
	else:
		dict_cor_row.update({NON_POLLUTED_DAYS_COUNT: -1})
		dict_cor_row.update({NON_POLLUTED_DAYS_PROPORTION: -1})
		dict_cor_row.update({KW_NONZERO_FOR_NON_POLLUTED_DAYS_COUNT: -1})
		dict_cor_row.update({KW_NON_POLLUTED_DAYS_PROPORTION: -1})

	trends_column: str
	if bool_ignore_zero:
		trends_column = trends_column_name_ignore_zero
	else:
		trends_column = target_variable_column_name_trends
	dict_cor_row.update({IGNORE_ZERO: bool_ignore_zero})

	df_trends_with_time_shift: pd.DataFrame = df_merged[trends_column].shift(periods=time_shift)

	if above_or_below_threshold == CORRELATE_ABOVE_THRESHOLD:
		df_epa_target_variable_above_or_below_threshold: pd.DataFrame = \
			df_merged[target_variable_column_name_epa].mask(df_merged[target_variable_column_name_epa] < threshold)
	elif above_or_below_threshold == CORRELATE_BELOW_THRESHOLD:
		df_epa_target_variable_above_or_below_threshold: pd.DataFrame = \
			df_merged[target_variable_column_name_epa].mask(df_merged[target_variable_column_name_epa] > threshold)
	else:
		df_epa_target_variable_above_or_below_threshold: pd.DataFrame = df_merged[target_variable_column_name_epa]

	# noinspection PyArgumentList
	# noinspection PyTypeChecker
	pearson_correlation: float = df_trends_with_time_shift.corr(
		df_epa_target_variable_above_or_below_threshold,
		method="pearson",
	)
	dict_cor_row.update({PEARSON_CORRELATION: pearson_correlation})

	# noinspection PyArgumentList
	# noinspection PyTypeChecker
	spearman_correlation: float = df_trends_with_time_shift.corr(
		df_epa_target_variable_above_or_below_threshold,
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
			log_error(error=f"city_mismatch{HYPHEN}{common_city}{HYPHEN}{nt_filename_trends_stitch_parsed.city}")
			continue
		if nt_filename_trends_stitch_parsed.common_word != common_word:
			log_error(error=f"word_mismatch{HYPHEN}{common_word}{HYPHEN}{nt_filename_trends_stitch_parsed.common_word}")
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

				df_common_word_cross_city_scalar: pd.DataFrame = df_common_city_keyword[COMMON_WORD_FREQUENCY] / df_city_to_be_scaled[COMMON_WORD_FREQUENCY]
				df_keyword_frequency_scaled_within_city: pd.DataFrame = df_city_to_be_scaled[COMMON_WORD_FREQUENCY] / df_city_to_be_scaled[COMMON_WORD_FREQUENCY_RELATIVE] * df_city_to_be_scaled[KEYWORD_FREQUENCY_RELATIVE]
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

				df_intercity.to_csv(
					f"{folder_stats_intercity}{common_city}{HYPHEN}{common_word}{HYPHEN}{nt_filename_trends_stitch_parsed.keyword}{HYPHEN}{city}{CSV}",
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
