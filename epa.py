import requests
import sys

from universal import *

# STATIC VARIABLES
EPA_API_CBSA: str = "cbsa"
EPA_API_DATA: str = 'Data'
EPA_API_DATE: str = "date_local"
EPA_API_DATE_FORMAT: str = "%Y%m%d"
EPA_API_START_DATE: str = "bdate"
EPA_API_END_DATE: str = "edate"
CITY_WIDE_AGGREGATION_SITE_NUMBER: int = -1
MAX_OF_DAILY_MEANS: str = "max_of_daily_means"
MEDIAN_OF_DAILY_MEANS: str = "median_of_daily_means"
MEAN_OF_DAILY_MEANS: str = "mean_of_daily_means"
MAX_OF_DAILY_MAXES: str = "max_of_daily_maxes"
MEDIAN_OF_DAILY_MAXES: str = "median_of_daily_maxes"
MEAN_OF_DAILY_MAXES: str = "mean_of_daily_maxes"

# PARAMS
PARAM_DOWNLOAD_DATA_TYPE: str = "download_data_type"

# NAMED TUPLES
NT_filename_epa_raw = namedtuple(
	"NT_filename_epa_raw",
	[
		CITY,
		POLLUTANT,
		START_DATE,
		END_DATE,
	]
)
NT_filename_epa_stitch = namedtuple(
	"NT_filename_epa_stitch",
	[
		CITY,
		POLLUTANT,
		TARGET_STATISTIC,
		EPA_COLUMN_SITE_NUMBER,
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
	with open(f"{EPA}{HYPHEN}{PARAMETERS}{JSON}") as json_file:
		json_data = json.load(json_file)
		aggregate: bool
		aggregate_and_upload: bool
		download: bool
		download_data_type: str
		only_download_missing: bool
		stitch: bool
		upload: bool
		list_partitioned_cities: Tuple[str, ...]
		list_pollutants: Tuple[str, ...]
		list_target_statistics: Tuple[str, ...]
		list_years: Tuple[int, ...]
		if called_from_main:
			aggregate = json_data[AGGREGATE]
			aggregate_and_upload = json_data[PARAM_AGGREGATE_AND_UPLOAD]
			download = json_data[DOWNLOAD]
			download_data_type = json_data[PARAM_DOWNLOAD_DATA_TYPE]
			only_download_missing = json_data[PARAM_ONLY_DOWNLOAD_MISSING]
			stitch = json_data[STITCH]
			upload = json_data[UPLOAD]
			credentials: str = json_data[PARAM_CREDENTIALS]
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
			list_target_statistics = parameters[TARGET_STATISTIC]
			list_years = parameters[YEAR]
		else:
			aggregate = False
			aggregate_and_upload = False
			download = False
			download_data_type = ""
			only_download_missing = True
			stitch = True
			upload = False
			list_partitioned_cities = list_cities
			list_pollutants = ()
			list_years = ()
	json_file.close()

	if download:
		set_error_task_origin(task_origin=DOWNLOAD)
		api_url: str = api.get(download_data_type, "")
		if api_url:
			api_starter_params: dict = parse_api_credentials(filename=credentials)
			city: str
			for city in list_partitioned_cities:
				pollutant: str
				for pollutant in list_pollutants:
					download_epa(
						city=city,
						pollutant=pollutant,
						list_years=list_years,
						api_url=api_url,
						api_starter_params=api_starter_params,
						download_data_type=download_data_type,
						only_download_missing=only_download_missing,
						folder_epa_raw=FOLDER_EPA_RAW,
					)
		else:
			log_error(error=f"api_url_missing{HYPHEN}{download_data_type}")
		write_errors_to_disk(overwrite=False)

	if stitch:
		set_error_task_origin(task_origin=STITCH)
		city: str
		for city in list_partitioned_cities:
			pollutant: str
			for pollutant in list_pollutants:
				stitch_epa(
					city=city,
					pollutant=pollutant,
					list_target_statistics=tuple(list_target_statistics),
					folder_epa_raw=FOLDER_EPA_RAW,
					folder_epa_stitch=FOLDER_EPA_STITCH,
				)
		write_errors_to_disk()

	if upload or aggregate:
		filename_label: str = EPA
		folder_output_aggregate: str = FOLDER_EPA_AGGREGATE
		if aggregate:
			set_error_task_origin(task_origin=AGGREGATE)
			write_errors_to_disk(clear_task_origin=False)
			is_valid_for_aggregation: bool = check_partition_valid_for_aggregation(
				error_label=EPA,
				partition_group=get_partition_group(),
				partition_total=get_partition_total(),
			)
			if is_valid_for_aggregation:
				aggregate_data_in_folder(
					filename_label=filename_label,
					folder_input=FOLDER_EPA_STITCH,
					folder_output_aggregate=folder_output_aggregate,
					list_cities=list_partitioned_cities,
					upload=aggregate_and_upload,
				)
			write_errors_to_disk(overwrite=False)

		if upload and not (aggregate and aggregate_and_upload):
			set_error_task_origin(task_origin=UPLOAD)
			nt_filename_aggregate = NT_filename_aggregate(
				aggregate=AGGREGATE,
				filename_label=filename_label,
			)
			filename_upload: str = generate_filename(
				nt_filename=nt_filename_aggregate,
				delimiter=HYPHEN,
				extension=CSV,
				folder=folder_output_aggregate,
			)
			upload_to_bigquery(
				path=filename_upload,
				table_name=filename_label,
			)
			write_errors_to_disk()


def generate_api_parameters(
		api_call_type: str,
) -> dict:
	if api_call_type == CBSA:
		return {}
	else:
		return {}


def download_epa(
		city: str,
		pollutant: str,
		list_years: Tuple[int, ...],
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
			log_error(error=f"pollutant_param_not_found{HYPHEN}{param}")
			return
	else:
		log_error(error=f"pollutant_not_found{HYPHEN}{pollutant}")
		return

	if download_data_type == CBSA:
		city_dict: dict = DEFAULT_CITIES.get(city, {})
		if city_dict:
			cbsa: int = city_dict.get(CBSA, 0)
			if cbsa:
				api_params.update({EPA_API_CBSA: cbsa})
			else:
				log_error(error=f"{CBSA}_not_found{HYPHEN}{cbsa}")
				return
		else:
			log_error(error=f"city_not_found{HYPHEN}{city}")
			return

		list_already_downloaded_files: Generator[str, None, List[str]] = import_paths_from_folder(
			folder=folder_epa_raw,
			list_paths_filter_conditions=(city, pollutant,),
		)
		for year in list_years:
			first_day_in_year: datetime = datetime.datetime(year, 1, 1)
			last_day_in_year: datetime = datetime.datetime(year, 12, 31)
			nt_filename_epa_raw: tuple = NT_filename_epa_raw(
				city=city,
				pollutant=pollutant,
				start_date=generate_date_for_filename_output(first_day_in_year.strftime(DATE_FORMAT)),
				end_date=generate_date_for_filename_output(last_day_in_year.strftime(DATE_FORMAT)),
			)
			filename_epa_raw: str = generate_filename(
				nt_filename=nt_filename_epa_raw,
				delimiter=HYPHEN,
				extension=CSV,
			)
			if not only_download_missing or filename_epa_raw not in list_already_downloaded_files:
				log_error(f"{DOWNLOAD} : {city} : {pollutant} : {year}", log=True)
				api_params.update({EPA_API_START_DATE: first_day_in_year.strftime(EPA_API_DATE_FORMAT)})
				api_params.update({EPA_API_END_DATE: last_day_in_year.strftime(EPA_API_DATE_FORMAT)})
				response: requests.Response = requests.get(
					url=api_url,
					params=api_params,
				)

				if response.status_code != 200:
					log_error(error=f"{filename_epa_raw}{HYPHEN}{response.status_code}")
					log_error(error=f"{filename_epa_raw}{HYPHEN}{response.headers}")
					continue

				response_dict: dict = json.loads(response.text)
				data_dict: dict = response_dict.get(EPA_API_DATA, {})
				if data_dict:
					df: pd.DataFrame = pd.DataFrame.from_dict(
						data_dict,
					)
					df.to_csv(
						f"{folder_epa_raw}{filename_epa_raw}",
						index=False,
					)
				else:
					log_error(error=f"missing_data{HYPHEN}{filename_epa_raw}")


def stitch_epa(
		city: str,
		pollutant: str,
		list_target_statistics: Tuple[str, ...] = DEFAULT_TARGET_STATISTICS,
		folder_epa_raw: str = FOLDER_EPA_RAW,
		folder_epa_stitch: str = FOLDER_EPA_STITCH,
) -> None:
	log_error(f"{STITCH} : {city} : {pollutant}", log=True)

	generate_sub_paths_for_folder(
		folder=folder_epa_stitch,
	)
	list_filenames: Generator[str, None, List[str]] = import_paths_from_folder(
		folder=folder_epa_raw,
		list_paths_filter_conditions=(city, pollutant, CSV),
	)
	filename: str
	list_dfs: List[pd.DataFrame] = []
	list_dates: List[str] = []
	for filename in list_filenames:
		nt_filename_epa_raw_parsed = parse_filename(
			filename=filename,
			named_tuple=NT_filename_epa_raw,
			delimiter=HYPHEN,
			extension=CSV,
		)
		list_dates.append(nt_filename_epa_raw_parsed.start_date)
		list_dates.append(nt_filename_epa_raw_parsed.end_date)

		df_empty_full_timeline: pd.DataFrame = pd.read_csv(
			f"{folder_epa_raw}{filename}",
			parse_dates=[EPA_COLUMN_DATE_LOCAL],
			infer_datetime_format=True,
		)
		filter_conditions: dict = DEFAULT_POLLUTANTS.get(pollutant, {}).get(EPA_FILTER, {})
		df_empty_full_timeline = filter_epa(
			df=df_empty_full_timeline,
			filter_conditions=filter_conditions,
		)
		df_empty_full_timeline = df_empty_full_timeline[EPA_COLUMNS]
		list_dfs.append(df_empty_full_timeline)

	if list_dates:
		list_dates.sort()
		first_date: str = list_dates[0]
		end_date: str = list_dates[-1]

		df_stitched: pd.DataFrame = pd.concat(
			list_dfs,
			ignore_index=True,
		)
		df_stitched.rename(
			columns={
				EPA_API_DATE: DATE,
			},
			inplace=True,
		)
		df_stitched = df_stitched.groupby([DATE, EPA_COLUMN_SITE_NUMBER]).agg('mean')
		df_stitched.reset_index(
			inplace=True,
			drop=False,
		)

		df_empty_full_timeline: pd.DataFrame = generate_empty_time_series_df(
			start_date=first_date.replace(UNDERSCORE, HYPHEN),
			end_date=end_date.replace(UNDERSCORE, HYPHEN),
		)
		df_empty_full_timeline: pd.DataFrame = pd.DataFrame({
			DATE: pd.to_datetime(df_empty_full_timeline[DATE]),
		})
		df_empty_full_timeline.set_index(
			DATE,
			inplace=True,
			drop=True,
		)

		for site_number, group in df_stitched.groupby([EPA_COLUMN_SITE_NUMBER]):
			target_statistic: str
			for target_statistic in list_target_statistics:
				log_error(f"{STITCH} : {city} : {pollutant} : {site_number} : {target_statistic}", log=True)
				clean_epa_df(
					df_to_clean=group.set_index(
						DATE,
						drop=True,
					),
					df_empty_full_timeline=df_empty_full_timeline,
					df_site_count=pd.Series(),
					city=city,
					pollutant=pollutant,
					target_statistic=target_statistic,
					site_number=int(site_number),
					first_date=first_date,
					end_date=end_date,
					folder_epa_stitch=folder_epa_stitch,
				)

		# noinspection PyUnresolvedReferences
		df_grouped: pd.DataFrameGroupBy = df_stitched.groupby([DATE])
		df_site_count: pd.Series = df_grouped.size()
		df_city_wide: pd.DataFrame
		for df_city_wide, target_statistic in aggregate_across_citywide_epa_sites_citywide(
				df_grouped=df_grouped,
		):
			log_error(f"{STITCH} : {city} : {pollutant} : all : {target_statistic}", log=True)
			clean_epa_df(
				df_to_clean=df_city_wide.to_frame(),
				df_empty_full_timeline=df_empty_full_timeline,
				df_site_count=df_site_count,
				city=city,
				pollutant=pollutant,
				target_statistic=target_statistic,
				site_number=CITY_WIDE_AGGREGATION_SITE_NUMBER,
				first_date=first_date,
				end_date=end_date,
				folder_epa_stitch=folder_epa_stitch,
			)


def clean_epa_df(
		df_to_clean: pd.DataFrame,
		df_empty_full_timeline: pd.DataFrame,
		df_site_count: pd.Series,
		city: str,
		pollutant: str,
		target_statistic: str,
		site_number: int,
		first_date: str,
		end_date: str,
		folder_epa_stitch: str,
) -> None:
	if site_number == CITY_WIDE_AGGREGATION_SITE_NUMBER:
		df_target_statistic = df_to_clean
	else:
		if target_statistic == MAX:
			df_target_statistic = pd.DataFrame(df_to_clean[EPA_COLUMN_FIRST_MAX_VALUE])
		elif target_statistic == MEAN:
			df_target_statistic = pd.DataFrame(df_to_clean[EPA_COLUMN_ARITHMETIC_MEAN])
		else:
			log_error(error=f"unrecognized_{TARGET_STATISTIC}{HYPHEN}{target_statistic}")
			return

	df_target_statistic.rename(
		columns={
			df_target_statistic.columns[0]: POLLUTION_LEVEL,
		},
		inplace=True,
	)

	df: pd.DataFrame = df_empty_full_timeline.merge(
		right=df_target_statistic,
		how="left",
		left_index=True,
		right_on=DATE,
	)
	if not df_site_count.empty:
		df[SITE_COUNT] = df_site_count
	else:
		df[SITE_COUNT] = 1

	df.insert(1, EPA_COLUMN_SITE_NUMBER, site_number)
	df.insert(1, TARGET_STATISTIC, target_statistic)
	df.insert(1, POLLUTANT, pollutant)
	df.insert(1, CITY, city)

	nt_filename_epa_stitch: tuple = NT_filename_epa_stitch(
		city=city,
		pollutant=pollutant,
		target_statistic=target_statistic,
		site_number=generate_numeric_for_filename_output(site_number),
		start_date=first_date,
		end_date=end_date,
	)
	filename_epa_stitch: str = generate_filename(
		nt_filename=nt_filename_epa_stitch,
		extension=CSV,
		delimiter=HYPHEN,
	)
	df.to_csv(
		f"{folder_epa_stitch}{filename_epa_stitch}",
		index=True,
	)


def aggregate_across_citywide_epa_sites_citywide(
		df_grouped,
) -> Generator[Tuple[pd.DataFrame, str], None, None]:
	df_grouped_daily_mean: pd.DataFrame = df_grouped[EPA_COLUMN_ARITHMETIC_MEAN]
	df_grouped_daily_max: pd.DataFrame = df_grouped[EPA_COLUMN_FIRST_MAX_VALUE]
	yield df_grouped_daily_mean.agg(MAX), MAX_OF_DAILY_MEANS
	yield df_grouped_daily_mean.agg(MEDIAN), MEDIAN_OF_DAILY_MEANS
	yield df_grouped_daily_mean.agg(MEAN), MEAN_OF_DAILY_MEANS
	yield df_grouped_daily_max.agg(MAX), MAX_OF_DAILY_MAXES
	yield df_grouped_daily_max.agg(MEDIAN), MEDIAN_OF_DAILY_MAXES
	yield df_grouped_daily_max.agg(MEAN), MEAN_OF_DAILY_MAXES


def filter_epa(
		df: pd.DataFrame,
		filter_conditions: dict,
) -> pd.DataFrame:
	if not filter_conditions:
		log_error(error=f"filter_epa_df_conditions{HYPHEN}empty")
		return df
	else:
		column: str
		values: Any
		for column, values in filter_conditions.items():
			if isinstance(values, list):
				df = df[df[column].isin(values)]
			else:
				df = df[df[column] == values]
		return df


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
