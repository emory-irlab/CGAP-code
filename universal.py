import datetime
import itertools
import json
import os
import re
from collections import namedtuple
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterator
from typing import List
from typing import NamedTuple
from typing import Pattern
from typing import Tuple

import pandas as pd

# Data Frame + BigQuery Static Variables
CITY: str = "city"
COMMON_WORD: str = "common_word"
CORRELATIONS: str = "correlations"
DATE: str = "date"
EPA: str = "epa"
IGNORE_ZERO: str = "ignore_zero"
INTERCITY: str = "intercity"
GROUP_BY: str = "group_by"
KEYWORD: str = "keyword"
POLLUTANT: str = "pollutant"
SITE_COUNT: str = "site_count"
TABLE_NAME: str = "table_name"
TARGET_STATISTIC: str = "target_statistic"
THRESHOLD: str = "threshold"
THRESHOLD_SIDE: str = "threshold_side"
THRESHOLD_SOURCE: str = "threshold_source"
TIME_SHIFT: str = "time_shift"
TRENDS: str = "trends"

# STATIC VARIABLES
AGGREGATE: str = "aggregate"
API: str = "api"
CBSA: str = "CBSA"
CITY_AB: str = "abbreviation"
COMMON_WORD_FREQUENCY: str = "common_word_frequency"
COMMON_WORD_FREQUENCY_RELATIVE: str = "common_word_frequency_relative"
CSV: str = ".csv"
DATE_FORMAT: str = "%Y-%m-%d"
DMA: str = "DMA"
DOWNLOAD: str = "download"
EMPTY_STRING: str = ""
END_DATE: str = "end_date"
ERROR: str = "error"
FORWARD_SLASH: str = "/"
GOOGLE_GEO_CODE: str = "google_geo_code"
HYPHEN: str = "-"
JSON: str = ".json"
KEYWORD_FREQUENCY: str = "keyword_frequency"
KEYWORD_FREQUENCY_RELATIVE = "keyword_frequency_relative"
LOG: str = "log"
MAX: str = "max"
MEAN: str = "mean"
MEDIAN: str = "median"
MISSING: str = "missing"
PARAMETERS: str = "parameters"
POLLUTION_LEVEL: str = "pollution_level"
SINGLE_SPACE: str = " "
SOURCE: str = "source"
START_DATE: str = "start_date"
STATE_AB: str = "state_ab"
STATE_NAME: str = "state_name"
STD: str = "std"
STITCH: str = "stitch"
TIME_SHIFT_NEGATIVE_SYMBOL: str = "~"
TXT: str = ".txt"
UNDERSCORE: str = "_"
UNIVERSAL: str = "universal"
UNKNOWN: str = "unknown"
UNNAMED: str = "Unnamed: 0"
UPLOAD: str = "upload"
YEAR: str = "year"

# ERROR STATEMENTS
ERROR_PARTITION: str = "Not available with multi-partitioning. Running on partition 1."
ERROR_EMPTY: str = "empty_data_frame"

# JSON PARAMETERS
PARAM_AGGREGATE_AND_UPLOAD: str = "aggregate_and_upload"
PARAM_BDATE: str = "bdate"
PARAM_EDATE: str = "edate"
PARAM_CREDENTIALS: str = "credentials"
PARAM_BIGQUERY_DATASET: str = "bigquery_dataset"
PARAM_BIGQUERY_PROJECT: str = "bigquery_project"
PARAM_DATE_START: str = "start_dates"
PARAM_DATE_END: str = "end_dates"
PARAM_FOLDER_EPA_RAW: str = "folder_epa_raw"
PARAM_FOLDER_EPA_STITCH: str = "folder_epa_stitch"
PARAM_FOLDER_EPA_AGGREGATE: str = "folder_epa_aggregate"
PARAM_FOLDER_ERROR: str = "folder_error"
PARAM_FOLDER_KEYWORDS: str = "folder_keywords"
PARAM_FOLDER_KEYWORDS_GOOGLE: str = "folder_keywords_google"
PARAM_FOLDER_EXPANSION_RAW: str = "folder_expansion_raw"
PARAM_FOLDER_EXPANSION_AGGREGATE: str = "folder_expansion_aggregate"
PARAM_FOLDER_EXPANSION_PARENTS: str = "folder_expansion_parents"
PARAM_FOLDER_STATS_CORRELATIONS_RAW: str = "folder_correlations_raw"
PARAM_FOLDER_STATS_CORRELATIONS_AGGREGATE: str = "folder_correlations_aggregate"
PARAM_FOLDER_STATS_CORRELATIONS_COMPARISON: str = "folder_correlations_comparison"
PARAM_FOLDER_STATS_INTERCITY_RAW: str = "folder_intercity_raw"
PARAM_FOLDER_STATS_INTERCITY_AGGREGATE: str = "folder_intercity_aggregate"
PARAM_FOLDER_TRENDS_METRICS_RAW: str = "folder_trends_metrics_raw"
PARAM_FOLDER_TRENDS_METRICS_AGGREGATE: str = "folder_trends_metrics_aggregate"
PARAM_FOLDER_EPA_METRICS_RAW: str = "folder_epa_metrics_raw"
PARAM_FOLDER_EPA_METRICS_AGGREGATE: str = "folder_epa_metrics_aggregate"
PARAM_FOLDER_TRENDS_RAW: str = "folder_trends_raw"
PARAM_FOLDER_TRENDS_STITCH: str = "folder_trends_stitch"
PARAM_FOLDER_TRENDS_AGGREGATE: str = "folder_trends_aggregate"
PARAM_MAX_SEARCH_FREQUENCY: str = "max_search_frequency"
PARAM_ONLY_DOWNLOAD_MISSING: str = "only_download_missing"

# USER SET
ATLANTA: str = "atlanta"
BOSTON: str = "boston"
CHICAGO: str = "chicago"
DALLAS: str = "dallas"
HOUSTON: str = "houston"
LOS_ANGELES: str = "los_angeles"
MIAMI: str = "miami"
NEW_YORK: str = "new_york"
PHILADELPHIA: str = "philadelphia"
WASHINGTON: str = "washington"
USA: str = "usa"

CO: str = "CO"
NO2: str = "NO2"
O3: str = "O3"
PM25: str = "PM2.5"
PM10: str = "PM10"
SO2: str = "SO2"

# DEFAULT VARIABLES
DEFAULT_COMMON_WORD: str = "pins"
DEFAULT_TARGET_STATISTICS: Tuple[str, ...] = (
	MAX,
	MEAN,
)

DEFAULT_CITIES: Dict[str, dict] = {
	ATLANTA:      {
		CBSA:            12060,
		CITY_AB:         "ATL",
		DMA:             524,
		STATE_NAME:      "GA",
		GOOGLE_GEO_CODE: "1015254",
	},
	BOSTON:       {
		CBSA:            14460,
		CITY_AB:         "BOS",
		DMA:             506,
		STATE_NAME:      "MA",
		GOOGLE_GEO_CODE: "1018127"
	},
	CHICAGO:      {
		CBSA:            16980,
		CITY_AB:         "ORD",
		DMA:             602,
		STATE_NAME:      "IL",
		GOOGLE_GEO_CODE: "1016367",
	},
	DALLAS:       {
		CBSA:            19100,
		CITY_AB:         "DFW",
		DMA:             623,
		STATE_NAME:      "TX",
		GOOGLE_GEO_CODE: "1026339",
	},
	HOUSTON:      {
		CBSA:            26420,
		CITY_AB:         "IAH",
		DMA:             618,
		STATE_NAME:      "TX",
		GOOGLE_GEO_CODE: "1026481",
	},
	LOS_ANGELES:  {
		CBSA:            31080,
		CITY_AB:         "LAX",
		DMA:             803,
		STATE_NAME:      "CA",
		GOOGLE_GEO_CODE: "1013962",
	},
	MIAMI:        {
		CBSA:            33100,
		CITY_AB:         "MIA",
		DMA:             528,
		STATE_NAME:      "FL",
		GOOGLE_GEO_CODE: "1015116",
	},
	NEW_YORK:     {
		CBSA:            35620,
		CITY_AB:         "NYC",
		DMA:             501,
		STATE_NAME:      "NY",
		GOOGLE_GEO_CODE: "1023191",
	},
	PHILADELPHIA: {
		CBSA:            37980,
		CITY_AB:         "PHL",
		DMA:             504,
		STATE_NAME:      "PA",
		GOOGLE_GEO_CODE: "1025197",
	},
	WASHINGTON:   {  # DC
		CBSA:            47900,
		CITY_AB:         "IAD",
		DMA:             511,
		STATE_NAME:      "DC",
		GOOGLE_GEO_CODE: "1014895",
	},
	USA:          {
		GOOGLE_GEO_CODE: "2840",
	},
}

EPA_FILTER: str = "filter"
EPA_API_POLLUTANT_PARAM: str = "param"
EPA_COLUMN_ARITHMETIC_MEAN: str = "arithmetic_mean"
EPA_COLUMN_DATE_LOCAL: str = "date_local"
EPA_COLUMN_FIRST_MAX_HOUR: str = "first_max_hour"
EPA_COLUMN_FIRST_MAX_VALUE: str = "first_max_value"
EPA_COLUMN_PARAMETER_CODE: str = "parameter_code"
EPA_COLUMN_SAMPLE_DURATION: str = "sample_duration"
EPA_COLUMN_SITE_NUMBER: str = "site_number"
EPA_COLUMNS: List[str] = [
	EPA_COLUMN_DATE_LOCAL,
	EPA_COLUMN_SITE_NUMBER,
	EPA_COLUMN_ARITHMETIC_MEAN,
	EPA_COLUMN_FIRST_MAX_VALUE,
]

DEFAULT_POLLUTANTS: Dict[str, dict] = {
	CO:   {
		EPA_API_POLLUTANT_PARAM: 42101,
		EPA_FILTER:              {
			EPA_COLUMN_PARAMETER_CODE:  42101,
			EPA_COLUMN_SAMPLE_DURATION: "8-HR RUN AVG END HOUR",
		},
	},
	NO2:  {
		EPA_API_POLLUTANT_PARAM: 42602,
		EPA_FILTER:              {
			EPA_COLUMN_PARAMETER_CODE:  42602,
			EPA_COLUMN_SAMPLE_DURATION: "1 HOUR",
		},
	},
	O3:   {
		EPA_API_POLLUTANT_PARAM: 44201,
		EPA_FILTER:              {
			EPA_COLUMN_PARAMETER_CODE:  44201,
			EPA_COLUMN_SAMPLE_DURATION: "8-HR RUN AVG BEGIN HOUR",
		},
	},
	PM25: {
		EPA_API_POLLUTANT_PARAM: 88101,
		EPA_FILTER:              {
			EPA_COLUMN_PARAMETER_CODE:  88101,
			EPA_COLUMN_SAMPLE_DURATION: [
				"24-HR BLK AVG",
				"24 HOUR",
			],
		},
	},
	PM10: {
		EPA_API_POLLUTANT_PARAM: 81102,
		EPA_FILTER:              {
			EPA_COLUMN_PARAMETER_CODE:  81102,
			EPA_COLUMN_SAMPLE_DURATION: "24 HOUR",
		},
	},
	SO2:  {
		EPA_API_POLLUTANT_PARAM: 42401,
		EPA_FILTER:              {
			EPA_COLUMN_PARAMETER_CODE:  42401,
			EPA_COLUMN_SAMPLE_DURATION: "1 HOUR",
		},
	}
}

# GLOBAL NAMED TUPLES
NT_filename_aggregate = namedtuple(
	"NT_filename_aggregate",
	[
		AGGREGATE,
		"filename_label",
	]
)
NT_filename_city_aggregate = namedtuple(
	"NT_filename_city_aggregate",
	[
		CITY,
	]
)
NT_date_pair = namedtuple(
	"NT_date_pair",
	[
		START_DATE,
		END_DATE,
	]
)
NT_filename_errors = namedtuple(
	"NT_filename_errors",
	[
		"error_file_origin",
		"error_task_origin",
	]
)
NT_error = namedtuple(
	"error",
	[
		"error",
	]
)

# GLOBAL VARIABLES
ERROR_FOLDER: str = ""
ERROR_LIST: List[str] = []
ERROR_FILE_ORIGIN: str = ""
ERROR_TASK_ORIGIN: str = ""
PARTITION_GROUP: int = 1
PARTITION_TOTAL: int = 1


def cast(
		cast_object: Any,
		cast_type: str,
) -> Any:
	if cast_type == "str":
		return str(cast_object)
	elif cast_type == "int":
		return int(cast_object)
	elif cast_type == "float":
		return float(cast_object)
	elif cast_type == "bool":
		return bool(cast_object)
	else:
		return cast_object


def generate_date_pair_for_full_series(
		list_date_pairs: List[Tuple[str, str]],
) -> Tuple[str, str]:
	first_end_date: str
	last_start_date: str

	first_start_date: str
	last_end_date: str
	first_start_date, first_end_date = list_date_pairs[0]
	last_start_date, last_end_date = list_date_pairs[-1]

	return NT_date_pair(start_date=first_start_date, end_date=last_end_date)


def generate_empty_time_series_df(
		start_date: str,
		end_date: str,
		date_format: str = DATE_FORMAT,
) -> pd.DataFrame:
	s_dt: datetime = datetime.datetime.strptime(start_date, date_format)
	e_dt: datetime = datetime.datetime.strptime(end_date, date_format)
	delta: datetime.timedelta = e_dt - s_dt
	dates_list: List[str] = [
		(s_dt + datetime.timedelta(days=x)).strftime(date_format)
		for x in range(delta.days + 1)
	]

	return pd.DataFrame({DATE: dates_list})


def filter_date_for_df(
		df: pd.DataFrame,
		date_column_is_index: bool,
		start_date: str,
		end_date: str,
		date_column: str = "",
		date_format: str = DATE_FORMAT
) -> pd.DataFrame:
	start_date_dt: datetime = datetime.datetime.strptime(start_date, date_format)
	end_date_dt: datetime = datetime.datetime.strptime(end_date, date_format)
	if date_column_is_index:
		return df.loc[start_date_dt:end_date_dt]
	else:
		if date_column:
			return df[(df[date_column] >= start_date_dt) & (df[date_column] <= end_date_dt)]
		else:
			log_error(error=f"date_column_empty{HYPHEN}returning_full_df")
			return df


def generate_date_for_filename_output(
		date: str,
) -> str:
	return date.replace(HYPHEN, UNDERSCORE)


def parse_filename_date(
		date: str,
) -> str:
	return date.replace(UNDERSCORE, HYPHEN)


def generate_numeric_for_filename_output(
		numeric: Any,
) -> str:
	return str(numeric).zfill(2).replace(HYPHEN, TIME_SHIFT_NEGATIVE_SYMBOL)


def parse_filename_numeric(
		numeric: str,
		cast_type: str = "int",
) -> Any:
	return cast(
		cast_object=numeric.replace(TIME_SHIFT_NEGATIVE_SYMBOL, HYPHEN),
		cast_type=cast_type,
	)


def generate_filename(
		nt_filename: tuple,
		extension: str,
		delimiter: str = "",
		folder: str = "",
) -> str:
	output: str = delimiter.join(nt_filename)
	if folder:
		output = folder + output
	if extension:
		output = output + extension
	return output


def parse_filename(
		filename: str,
		delimiter: str,
		named_tuple,  # NamedTuple annotation doesn't work when using it as a callable
		extension: str = "",
		dt_dict: Dict[str, Any] = None,
) -> tuple:
	if extension:
		filename = filename.replace(extension, "")
	split_filename: List[str] = filename.split(delimiter)
	parsed_filename: NamedTuple
	try:
		parsed_filename = named_tuple(*split_filename)
	except TypeError:
		error: str = f"parse_filename{HYPHEN}named_tuple_size_mismatch{HYPHEN}filename"
		parsed_filename = NT_error(error)
		log_error(error=error)
	if dt_dict:
		parsed_filename_dict: Dict[str, Any] = parsed_filename._asdict()
		list_casted_data: List[Any] = []
		for key, value in parsed_filename_dict.items():
			cast_type: Any = dt_dict.get(key, None)
			if cast_type:
				casted_value: Any
				if callable(cast_type):
					casted_value = cast_type(value)
				else:
					casted_value = cast(
						cast_object=value,
						cast_type=cast_type,
					)
				list_casted_data.append(casted_value)
			else:
				list_casted_data.append(value)
		return named_tuple(*list_casted_data)
	else:
		return parsed_filename


def aggregate_data_in_folder(
		filename_label: str,
		folder_input: str,
		folder_output_aggregate: str,
		list_cities: Tuple[str, ...] = tuple(DEFAULT_CITIES),
		bool_suppress_print: bool = False,
		upload: bool = False,
) -> None:
	generate_sub_paths_for_folder(
		folder=folder_output_aggregate,
	)
	list_data_dfs_for_all_cities: List[pd.DataFrame] = []
	city: str
	for city in list_cities:
		list_parsed_dfs_per_city: List[pd.DataFrame] = []
		filename: str
		for filename in import_paths_from_folder(
				folder=folder_input,
				list_paths_filter_conditions=(CSV, city),
		):
			# todo - exception handling - pass in column mapping to create empty row
			# noinspection PyUnresolvedReferences
			try:
				df: pd.DataFrame = pd.read_csv(
					f"{folder_input}{filename}",
				)
				list_parsed_dfs_per_city.append(df)
			except pd.io.common.EmptyDataError:
				log_error(error=f"{ERROR_EMPTY}{HYPHEN}{AGGREGATE}{city}{HYPHEN}{filename}", bool_suppress_print=bool_suppress_print)
		if len(list_parsed_dfs_per_city) > 0:
			log_error(error=f"{AGGREGATE} : {city}", log=True)
			concatenated_data_per_city: pd.DataFrame = pd.concat(
				list_parsed_dfs_per_city,
				sort=True,
			)
			del list_parsed_dfs_per_city
			nt_filename_city_aggregate: tuple = NT_filename_city_aggregate(
				city=city,
			)
			filename_city_aggregate: str = generate_filename(
				nt_filename=nt_filename_city_aggregate,
				delimiter=HYPHEN,
				extension=CSV,
				folder=folder_output_aggregate,
			)
			concatenated_data_per_city.to_csv(
				filename_city_aggregate,
				index=False,
			)
			list_data_dfs_for_all_cities.append(concatenated_data_per_city)
		else:
			log_error(error=f"{city}{HYPHEN}{ERROR_EMPTY}")
		write_errors_to_disk(clear_task_origin=False, bool_suppress_print=True, overwrite=False)

	df_aggregate: pd.DataFrame = pd.concat(
		list_data_dfs_for_all_cities,
		sort=True,
	)
	del list_data_dfs_for_all_cities
	nt_filename_aggregate: tuple = NT_filename_aggregate(
		aggregate=AGGREGATE,
		filename_label=filename_label,
	)
	filename_aggregate: str = generate_filename(
		nt_filename=nt_filename_aggregate,
		delimiter=HYPHEN,
		extension=CSV,
		folder=folder_output_aggregate,
	)
	df_aggregate.to_csv(
		filename_aggregate,
		index=False,
	)
	if upload:
		upload_to_bigquery(
			filename=filename_aggregate,
			table_name=filename_label,
		)


def upload_to_bigquery(
		filename: str,
		table_name: str,
) -> None:
	log_error(f"Attempting upload to bigquery", log=True)
	from google.cloud import bigquery

	client = bigquery.Client.from_service_account_json(CREDENTIALS_BIGQUERY)

	cleaned_table_name: str = UNDERSCORE.join(table_name.split(UNDERSCORE)[1:])
	table_id = '.'.join([BIGQUERY_PROJECT, BIGQUERY_DATASET, cleaned_table_name])
	log_error(f"bigquery : {table_id}", log=True)
	job_config = bigquery.LoadJobConfig()

	# WRITE_EMPTY    : Writes the data only if the table is empty.
	# WRITE_APPEND   : Appends the data to the end of the table.
	# WRITE_TRUNCATE : Erases all existing data in a table before writing the new data.
	job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

	job_config.source_format = bigquery.SourceFormat.CSV
	# job_config.skip_leading_rows = 1
	job_config.autodetect = True

	with open(filename, "rb") as source_file:
		job = client.load_table_from_file(source_file, table_id, job_config=job_config)

	job.result()
	log_error(f"Loaded {job.output_rows} rows into {table_id}", log=True)


def flatten_list(
		list_of_lists: List[List[Any]],
) -> List[Any]:
	item: Any
	sublist: List[Any]
	flat_list: List[Any] = [
		item
		for sublist in list_of_lists for item in sublist
	]

	return flat_list


def partition_list(
		list_partition_candidates: List[Any],
		partition_group: int,
		partition_total: int,
) -> List[Any]:
	assert (partition_group > 0), f"The partition group must equal 1 or more"
	assert partition_group <= partition_total, f"Partition group: {partition_group} exceeds partition total {partition_total}."
	if partition_total == 1:

		return list_partition_candidates

	else:
		end_index: int
		start_index: int = (partition_group - 1) * (len(list_partition_candidates) // partition_total)
		if partition_group == partition_total:
			end_index = len(list_partition_candidates)
		else:
			end_index = partition_group * (len(list_partition_candidates) // partition_total)

		return list_partition_candidates[start_index:end_index]


def parse_api_credentials(
		filename: str,
) -> dict:
	try:
		with open(f"{filename}") as json_file:
			json_data = json.load(json_file)
			return json_data
	except FileNotFoundError:
		log_error(f"file_not_found{HYPHEN}{filename}")
		return {}


def is_single_item(
		list_items: List[Any],
) -> bool:
	if len(list_items) > 0:
		item, *list_rest = list_items
		if len(list_rest) != 0:
			print(
				f"Multiple item candidates; using the first candidate: {item}. These are the rest: {list_rest}."
			)
			extra_candidate: str
			for extra_candidate in list_rest:
				log_error(error=f"filter_single_item{HYPHEN}extra_candidate{HYPHEN}{extra_candidate}")
			return False
		else:
			return True
	else:
		log_error(error=f"filter_single_item{HYPHEN}list_empty")
		return False


def import_single_file(
		folder: str,
		list_filename_filter_conditions: Tuple[str, ...],
) -> str:
	filename: str
	list_rest: List[str]
	list_filenames: List[str] = import_paths_from_folder(
		folder=folder,
		list_paths_filter_conditions=list_filename_filter_conditions,
	)
	single_file: bool = is_single_item(list_filenames)
	if not single_file:
		log_error(error=f"parse_filename{HYPHEN}{'_'.join(list_filename_filter_conditions)}")
		return ""
	else:
		first_file: str
		rest_of_files: List[str]
		first_file, *rest_of_files = list_filenames
		return first_file


def import_paths_from_folder(
		folder: str,
		list_paths_filter_conditions: Tuple[str, ...] = (),
		check_paths: bool = False,
		include_files: bool = True,
		include_folders: bool = False,
		ignore_hidden: bool = True,
) -> List[str]:
	if os.path.exists(folder):
		list_paths: List[str] = os.listdir(folder)
		if len(list_paths_filter_conditions) > 0:
			filter_all_conditions_function: Callable[[str], bool] = lambda x: all(
				str(condition.lower()) in x
				for condition in list_paths_filter_conditions
			)
			list_paths = list(
				itertools.compress(
					list_paths,
					map(
						filter_all_conditions_function,
						(
							path.lower()
							for path in list_paths
						)
					)
				)
			)
		list_paths.sort()

		if check_paths:
			if include_files:
				list_paths = [
					path
					for path in list_paths
					if os.path.isfile(f"{folder}{path}")
				]
			if include_folders:
				list_paths = [
					path
					for path in list_paths
					if os.path.isdir(f"{folder}{path}")
				]
			if ignore_hidden:
				pattern_hidden_path: Pattern[str] = re.compile(r"\.[A-Za-z]")
				list_paths = [
					path
					for path in list_paths
					if not re.match(pattern_hidden_path, path)
				]

		return list_paths

	else:
		generate_sub_paths_for_folder(
			folder=folder,
		)
		return []


def generate_sub_paths_for_folder(
		folder: str,
) -> None:
	directories: List[str] = folder.split("/")
	recursive_sub_directories: Iterator[str] = itertools.accumulate(directories, lambda x, y: "/".join([x, y]))
	sub_directory: str
	for sub_directory in recursive_sub_directories:
		if not os.path.isdir(sub_directory):
			os.mkdir(sub_directory)


def filter_list_strings(
		list_strings: List[str],
		list_string_filter_conditions: Tuple[str, ...] = (),
) -> List[str]:
	if len(list_string_filter_conditions) > 0:
		list_strings = list(
			filter(
				lambda x: all(
					str(condition) in x
					for condition in list_string_filter_conditions
				),
				list_strings
			)
		)
		list_strings.sort()

	return list_strings


def write_list_to_file(
		filename: str,
		folder: str,
		list_strings: List[str],
) -> None:
	output_filename: str = f"{folder}{filename}"
	output_write_type: str
	if os.path.exists(output_filename):
		output_write_type = "a+"
	else:
		output_write_type = "w+"
		generate_sub_paths_for_folder(
			folder=folder,
		)

	with open(output_filename, output_write_type) as output_file:
		for string in list_strings:
			if string not in output_file:
				output_file.write(str(string))
				output_file.write("\n")
	output_file.close()


def check_partition_valid_for_aggregation(
		error_label: str,
		partition_group: int,
		partition_total: int,
) -> bool:
	if get_partition_group() > 1 or get_partition_total() > 1:
		log_error(error=f"{ERROR_PARTITION}{HYPHEN}{error_label}")
		print(f"Can only aggregate {error_label} when run as a SINGLE partition group AND with ONE total partition due to race conditions. Not guaranteed that the other partitions have finished running; please check and rerun. Current partition group: {partition_group}. Current partition total: {partition_total}.")
		return False
	else:
		return True


def get_partition_group() -> int:
	return PARTITION_GROUP


def get_partition_total() -> int:
	return PARTITION_TOTAL


def set_partition_total(
		partition_total: int = 1,
) -> None:
	global PARTITION_TOTAL
	assert partition_total > 0, f"Partition total must be above 0"
	PARTITION_TOTAL = partition_total
	assert PARTITION_TOTAL >= PARTITION_GROUP, f"Partition total must be greater than or equal to the partition groups"


def set_partition_group(
		partition_group: int = 1,
) -> None:
	global PARTITION_GROUP
	assert partition_group > 0, f"Partition group must be above 0"
	PARTITION_GROUP = partition_group


def log_error(
		error: str,
		log: bool = False,
		bool_suppress_print: bool = False,
) -> None:
	global ERROR_LIST
	if error:
		ERROR_LIST.append(error)
	if not bool_suppress_print:
		if log:
			print(f"{LOG} : {error}")
		else:
			print(f"{ERROR} : {error}")


def set_error_folder(
		error_folder: str,
) -> None:
	global ERROR_FOLDER
	ERROR_FOLDER = error_folder
	print(f"error_folder : {error_folder}")


def set_error_file_origin(
		file_origin: str,
) -> None:
	global ERROR_FILE_ORIGIN
	ERROR_FILE_ORIGIN = file_origin
	print(f"file_origin : {file_origin}")


def set_error_task_origin(
		task_origin: str,
) -> None:
	global ERROR_TASK_ORIGIN
	ERROR_TASK_ORIGIN = task_origin
	if task_origin:
		print(f"task_origin : {task_origin}")


def write_errors_to_disk(
		clear_task_origin: bool = True,
		folder_error: str = ERROR_FOLDER,
		bool_suppress_print: bool = False,
		overwrite: bool = True,
) -> None:
	global ERROR_LIST
	if not folder_error:
		folder_error = ERROR_FOLDER
	error_task_origin: str
	error_file_origin: str
	if ERROR_FILE_ORIGIN:
		error_file_origin = ERROR_FILE_ORIGIN
	else:
		error_file_origin = UNKNOWN
		print(f"No file origin was specified for the error log.")
	if ERROR_TASK_ORIGIN:
		error_task_origin = ERROR_TASK_ORIGIN
	else:
		error_task_origin = UNKNOWN
		print(f"No task origin was specified for the error log.")

	output_filename: str = generate_filename(
		nt_filename=NT_filename_errors(
			error_file_origin=error_file_origin,
			error_task_origin=error_task_origin,
		),
		delimiter=HYPHEN,
		folder=folder_error,
		extension=TXT,
	)
	output_write_type: str
	if overwrite:
		output_write_type = "w+"
	elif os.path.exists(output_filename):
		output_write_type = "a+"
	else:
		output_write_type = "w+"

	with open(output_filename, output_write_type) as error_log_file:
		current_datetime: str = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
		error_log_file.write(f"{current_datetime} : {error_task_origin}")
		error_log_file.write("\n")

		error: str
		for error in ERROR_LIST:
			error_log_file.write(str(error))
			error_log_file.write("\n")
		error_log_file.write("\n")
		error_log_file.write("\n")

	ERROR_LIST = []
	if clear_task_origin:
		set_error_task_origin("")
	if not bool_suppress_print:
		print(f"Error log outputted to: {output_filename}")


if __name__ == "__main__":
	print(f"universal.py is not meant to be run as a script. Aborting.")
	exit(1)

with open(f"{UNIVERSAL}{HYPHEN}{PARAMETERS}{JSON}") as universal_json_file:
	universal_parameters = json.load(universal_json_file)
	BIGQUERY_PROJECT: str = universal_parameters[PARAM_BIGQUERY_PROJECT]
	BIGQUERY_DATASET: str = universal_parameters[PARAM_BIGQUERY_DATASET]
	CREDENTIALS_BIGQUERY: str = universal_parameters[PARAM_CREDENTIALS]
	COMMON_WORD_UNIVERSAL: str = universal_parameters[COMMON_WORD]
	MAX_SEARCH_VOLUME: float = universal_parameters[PARAM_MAX_SEARCH_FREQUENCY]
	START_DATES: str = universal_parameters[PARAM_DATE_START]
	END_DATES: str = universal_parameters[PARAM_DATE_END]
	FOLDER_EPA_RAW: str = universal_parameters[PARAM_FOLDER_EPA_RAW]
	FOLDER_EPA_STITCH: str = universal_parameters[PARAM_FOLDER_EPA_STITCH]
	FOLDER_EPA_AGGREGATE: str = universal_parameters[PARAM_FOLDER_EPA_AGGREGATE]
	FOLDER_EPA_METRICS_RAW: str = universal_parameters[PARAM_FOLDER_EPA_METRICS_RAW]
	FOLDER_EPA_METRICS_AGGREGATE: str = universal_parameters[PARAM_FOLDER_EPA_METRICS_AGGREGATE]
	FOLDER_ERROR: str = universal_parameters[PARAM_FOLDER_ERROR]
	FOLDER_EXPANSION_RAW = universal_parameters[PARAM_FOLDER_EXPANSION_RAW]
	FOLDER_EXPANSION_AGGREGATE = universal_parameters[PARAM_FOLDER_EXPANSION_AGGREGATE]
	FOLDER_EXPANSION_PARENTS = universal_parameters[PARAM_FOLDER_EXPANSION_PARENTS]
	FOLDER_KEYWORDS_GOOGLE = universal_parameters[PARAM_FOLDER_KEYWORDS_GOOGLE]
	FOLDER_KEYWORDS: str = universal_parameters[PARAM_FOLDER_KEYWORDS]
	FOLDER_STATS_INTERCITY_RAW: str = universal_parameters[PARAM_FOLDER_STATS_INTERCITY_RAW]
	FOLDER_STATS_INTERCITY_AGGREGATE: str = universal_parameters[PARAM_FOLDER_STATS_INTERCITY_AGGREGATE]
	FOLDER_TRENDS_RAW: str = universal_parameters[PARAM_FOLDER_TRENDS_RAW]
	FOLDER_TRENDS_STITCH: str = universal_parameters[PARAM_FOLDER_TRENDS_STITCH]
	FOLDER_TRENDS_AGGREGATE: str = universal_parameters[PARAM_FOLDER_TRENDS_AGGREGATE]
	FOLDER_TRENDS_METRICS_RAW: str = universal_parameters[PARAM_FOLDER_TRENDS_METRICS_RAW]
	FOLDER_TRENDS_METRICS_AGGREGATE: str = universal_parameters[PARAM_FOLDER_TRENDS_METRICS_AGGREGATE]

	FOLDER_STATS_CORRELATIONS_RAW: str = universal_parameters[PARAM_FOLDER_STATS_CORRELATIONS_RAW]
	FOLDER_STATS_CORRELATIONS_AGGREGATE: str = universal_parameters[PARAM_FOLDER_STATS_CORRELATIONS_AGGREGATE]
	FOLDER_STATS_CORRELATIONS_COMPARISON: str = universal_parameters[PARAM_FOLDER_STATS_CORRELATIONS_COMPARISON]
universal_json_file.close()

FULL_START_DATE: str
FULL_END_DATE: str
LIST_DATE_PAIRS: List[Tuple[str, str]] = list(zip(START_DATES, END_DATES))
FULL_START_DATE, FULL_END_DATE = generate_date_pair_for_full_series(list_date_pairs=LIST_DATE_PAIRS)
