import functools

from universal import *

# STATIC VARIABLES
BING: str = "bing"
COLUMNS_TO_DROP: str = "columns_to_drop"
COVID: str = "covid"
DATE_COLUMN: str = "date_column"
DTYPE: str = "dtype"
FILE_PATH_OR_FILTER_NAME_CONDITIONS: str = "file_path_or_filter_name_conditions"
FOLDER_PATH: str = "folder_path"
GOOGLE: str = "google"
MAPPING: str = "mapping"

# PARAMS
PARAM_BING_DATA_FILEPATH: str = "bing_data_file_path"
PARAM_COLUMNS_TO_DROP: str = "columns_to_drop"
PARAM_DATA: str = "data"
PARAM_DTYPES: str = "dtypes"
PARAM_FOLDER_COVID_RAW: str = "folder_covid_raw"
PARAM_FOLDER_COVID_AGGREGATE: str = "folder_covid_aggregate"
PARAM_GOOGLE_DATA_FILENAME_FILTER_CONDITIONS: str = "google_data_filename_filter_conditions"
PARAM_GOOGLE_DTYPES: str = "google_dtypes"
PARAM_INNER_OR_OUTER_JOIN: str = "inner_or_outer_join"

# DEFAULTS
RE_camel_to_snake_case_pattern: Pattern = re.compile(r"(?<!^)(?=[A-Z])")


def main(
		called_from_main: bool = False,
) -> None:
	set_error_file_origin(COVID)
	set_error_folder(FOLDER_ERROR)
	if called_from_main:
		with open(f"{COVID}{HYPHEN}{PARAMETERS}{JSON}") as json_file:
			json_data = json.load(json_file)
			data_schema: dict = json_data[PARAM_DATA]
			folder_covid_raw: str = json_data[PARAM_FOLDER_COVID_RAW]
			folder_covid_stitch: str = json_data[PARAM_FOLDER_COVID_AGGREGATE]
			inner_or_outer_join: str = json_data[PARAM_INNER_OR_OUTER_JOIN]
		json_file.close()
	else:
		pass

	set_error_task_origin(AGGREGATE)
	list_parsed_dfs: List[pd.DataFrame] = parse_data(
		data_schema=data_schema,
		folder=folder_covid_raw,
	)
	df_merged: pd.DataFrame = functools.reduce(
		lambda df_1, df_2: pd.merge(
			left=df_1,
			right=df_2,
			how=inner_or_outer_join,
		),
		list_parsed_dfs,
	)

	generate_sub_paths_for_folder(
		folder=folder_covid_stitch,
	)
	nt_filename_aggregate = NT_filename_aggregate(
		aggregate=AGGREGATE,
		filename_label=COVID,
	)
	filename_covid_stitch: str = generate_filename(
		nt_filename=nt_filename_aggregate,
		extension=CSV,
	)
	df_merged.to_csv(
		f"{folder_covid_stitch}{filename_covid_stitch}",
		index=False,
	)
	write_errors_to_disk()


def parse_data(
		data_schema: dict,
		folder: str,
) -> List[pd.DataFrame]:
	list_df: List[pd.DataFrame] = []

	source: str
	data_scheme: dict
	for source, data_scheme in data_schema.items():
		filepath: Any = data_scheme.get(FILE_PATH_OR_FILTER_NAME_CONDITIONS, None)
		if isinstance(filepath, str):
			pass
		elif isinstance(filepath, list):
			filepath = import_single_file(
				folder=folder,
				list_filename_filter_conditions=tuple(filepath),
			)
		else:
			log_error(
				error=(
					f"{source}{HYPHEN}"
					f"{FILE_PATH_OR_FILTER_NAME_CONDITIONS}{HYPHEN}"
					f"neither_a_list_or_string"
				)
			)
			continue

		df: pd.DataFrame
		date_column: str = data_scheme.get(DATE_COLUMN, "")
		if not date_column:
			log_error(error=f"{source}{HYPHEN}{DATE_COLUMN}{HYPHEN}{MISSING}")
			continue
		dtype: dict = data_scheme.get(DTYPE, {})
		if dtype:
			df = pd.read_csv(
				f"{folder}{filepath}",
				dtype=dtype,
				parse_dates=[date_column],
				infer_datetime_format=True,
			)
		else:
			df = pd.read_csv(
				f"{folder}{filepath}",
				parse_dates=[date_column],
				infer_datetime_format=True,
			)

		columns_to_drop: List[str] = data_scheme.get(COLUMNS_TO_DROP, [])
		if columns_to_drop:
			df.drop(
				columns=filter(None, columns_to_drop),
				inplace=True,
			)

		mapping: dict = data_scheme.get(MAPPING, {})
		if mapping:
			df.rename(
				columns=mapping,
				inplace=True,
			)

		columns_to_snake_case_mapping: dict = {
			column_name: RE_camel_to_snake_case_pattern.sub("_", column_name).lower()
			for column_name in df.columns
		}
		df.rename(
			columns=columns_to_snake_case_mapping,
			inplace=True,
		)
		list_df.append(df)

	return list_df


main(
	*set_up_main(
		name=__name__,
		possible_number_of_input_arguments=1,
	),
)
