import itertools
import json
import os
import re
from collections import namedtuple
from datetime import datetime
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterator
from typing import List
from typing import NamedTuple
from typing import Pattern
from typing import Tuple

import pandas as pd

# STATIC VARIABLES
AGGREGATE: str = 'aggregate'
API: str = 'api'
CITY: str = 'city'
CITY_AB: str = 'abbreviation'
COMMON_WORD: str = 'common_word'
COMMON_WORD_FREQUENCY: str = 'common_word_frequency'
COMMON_WORD_FREQUENCY_RELATIVE: str = 'common_word_frequency_relative'
CREDENTIALS: str = 'credentials'
CSV: str = '.csv'
DATE: str = 'date'
DATE_FORMAT: str = '%Y-%m-%d'
DMA: str = 'DMA'
DOWNLOAD: str = 'download'
EMPTY_STRING: str = ''
END_DATE: str = 'end_date'
EPA: str = 'epa'
FORWARD_SLASH: str = '/'
GOOGLE_GEO_CODE: str = 'google_geo_code'
HYPHEN: str = '-'
IGNORE_ZERO: str = 'ignore_zero'
JSON: str = '.json'
KEYWORD: str = 'keyword'
KEYWORD_FREQUENCY: str = 'keyword_frequency'
KEYWORD_FREQUENCY_RELATIVE = 'keyword_frequency_relative'
LOG: str = 'log'
MAX: str = 'max'
MEAN: str = 'mean'
MEDIAN: str = 'median'
MISSING: str = 'missing'
PARAMETERS: str = 'parameters'
POLLUTANT: str = 'pollutant'
POLLUTION_LEVEL: str = 'pollution_level'
SINGLE_SPACE: str = ' '
START_DATE: str = 'start_date'
STATE_AB: str = 'state_ab'
STATE_NAME: str = 'state_name'
STITCH: str = 'stitch'
THRESHOLD: str = 'threshold'
THRESHOLD_PERCENTILE: str = 'threshold_percentile'
THRESHOLD_SIDE: str = 'threshold_side'
TIME_SHIFT: str = 'time_shift'
TRENDS: str = 'trends'
UNDERSCORE: str = '_'
UNIVERSAL: str = 'universal'
UNKNOWN: str = 'unknown'
UNNAMED: str = 'Unnamed: 0'
SOURCE: str = 'source'
TARGET_STATISTIC: str = 'target_statistic'
TIME_SHIFT_NEGATIVE_SYMBOL: str = '~'
TXT: str = '.txt'

# ERROR STATEMENTS
ERROR_PARTITION: str = 'not available with multi-partitioning. Running on partition 1.'
ERROR_EMPTY: str = 'empty_data_frame'

# JSON PARAMETERS
PARAM_DATE_START: str = 'start_dates'
PARAM_DATE_END: str = 'end_dates'
PARAM_FOLDER_EPA_RAW: str = 'folder_epa_raw'
PARAM_FOLDER_EPA_STITCH: str = 'folder_epa_stitch'
PARAM_FOLDER_EPA_AGGREGATE: str = 'folder_epa_aggregate'
PARAM_FOLDER_ERROR: str = 'folder_error'
PARAM_FOLDER_KEYWORDS: str = 'folder_keywords'
PARAM_FOLDER_KEYWORDS_GOOGLE: str = 'folder_keywords_google'
PARAM_FOLDER_EXPANSION_RAW: str = 'folder_expansion_raw'
PARAM_FOLDER_EXPANSION_AGGREGATE: str = 'folder_expansion_aggregate'
PARAM_FOLDER_EXPANSION_PARENTS: str = 'folder_expansion_parents'
PARAM_FOLDER_STATS_CORRELATIONS_RAW: str = 'folder_stats_correlations_raw'
PARAM_FOLDER_STATS_CORRELATIONS_AGGREGATE: str = 'folder_stats_correlations_aggregate'
PARAM_FOLDER_STATS_CORRELATIONS_COMPARISON: str = 'folder_stats_correlations_comparison'
PARAM_FOLDER_STATS_INTERCITY_RAW: str = 'folder_stats_intercity_raw'
PARAM_FOLDER_STATS_INTERCITY_AGGREGATE: str = 'folder_stats_intercity_aggregate'
PARAM_FOLDER_TRENDS_METRICS_RAW: str = 'folder_trends_metrics_raw'
PARAM_FOLDER_TRENDS_METRICS_AGGREGATE: str = 'folder_trends_metrics_aggregate'
PARAM_FOLDER_EPA_METRICS_RAW: str = 'folder_epa_metrics_raw'
PARAM_FOLDER_EPA_METRICS_AGGREGATE: str = 'folder_epa_metrics_aggregate'
PARAM_FOLDER_TRENDS_RAW: str = 'folder_trends_raw'
PARAM_FOLDER_TRENDS_STITCH: str = 'folder_trends_stitch'
PARAM_FOLDER_TRENDS_AGGREGATE: str = 'folder_trends_aggregate'
PARAM_MAX_SEARCH_FREQUENCY: str = 'max_search_frequency'

# USER SET
ATLANTA: str = 'atlanta'
BOSTON: str = 'boston'
CHICAGO: str = 'chicago'
DALLAS: str = 'dallas'
HOUSTON: str = 'houston'
LOS_ANGELES: str = 'los_angeles'
MIAMI: str = 'miami'
NEW_YORK: str = 'new_york'
PHILADELPHIA: str = 'philadelphia'
WASHINGTON: str = 'washington'
USA: str = 'usa'

CO: str = 'CO'
NO2: str = 'NO2'
O3: str = 'O3'
PM25: str = 'PM2.5'
PM10: str = 'PM10'
SO2: str = 'SO2'

# DEFAULT VARIABLES
DEFAULT_COMMON_WORD: str = 'pins'
DEFAULT_AGGREGATE_FILENAME_FILTER_CONDITIONS: Tuple[str, str] = (
    AGGREGATE,
    CSV,
)

DEFAULT_CITIES: Dict[str, dict] = {
    ATLANTA:      {
        CITY_AB:         'ATL',
        DMA:             524,
        STATE_NAME:      'GA',
        GOOGLE_GEO_CODE: '1015254',
    },
    BOSTON:       {
        CITY_AB:         'BOS',
        DMA:             506,
        STATE_NAME:      'MA',
        GOOGLE_GEO_CODE: '1018127'
    },
    CHICAGO:      {
        CITY_AB:         'ORD',
        DMA:             602,
        STATE_NAME:      'IL',
        GOOGLE_GEO_CODE: '1016367',
    },
    DALLAS:       {
        CITY_AB:         'DFW',
        DMA:             623,
        STATE_NAME:      'TX',
        GOOGLE_GEO_CODE: '1026339',
    },
    HOUSTON:      {
        CITY_AB:         'IAH',
        DMA:             618,
        STATE_NAME:      'TX',
        GOOGLE_GEO_CODE: '1026481',
    },
    LOS_ANGELES:  {
        CITY_AB:         'LAX',
        DMA:             803,
        STATE_NAME:      'CA',
        GOOGLE_GEO_CODE: '1013962',
    },
    MIAMI:        {
        CITY_AB:         'MIA',
        DMA:             528,
        STATE_NAME:      'FL',
        GOOGLE_GEO_CODE: '1015116',

    },
    NEW_YORK:     {
        CITY_AB:         'NYC',
        DMA:             501,
        STATE_NAME:      'NY',
        GOOGLE_GEO_CODE: '1023191',
    },
    PHILADELPHIA: {
        CITY_AB:         'PHL',
        DMA:             504,
        STATE_NAME:      'PA',
        GOOGLE_GEO_CODE: '1025197',
    },
    WASHINGTON:   {  # DC
        CITY_AB:         'IAD',
        DMA:             511,
        STATE_NAME:      'DC',
        GOOGLE_GEO_CODE: '1014895'

    },
    USA:          {
        GOOGLE_GEO_CODE: '2840'
    },
}

DEFAULT_POLLUTANTS: Dict[str, dict] = {
    CO:   {
    },
    NO2:  {
    },
    O3:   {
    },
    PM25: {
    },
    PM10: {
    },
    SO2:  {
    },
}

# GLOBAL NAMED TUPLES
NT_aggregate_filename = namedtuple(
    'NT_aggregate',
    [
        AGGREGATE,
    ]
)
NT_city_aggregate_filename = namedtuple(
    'NT_filename_city_aggregate',
    [
        CITY,
    ]
)
NT_date_pair = namedtuple(
    'NT_date_pair',
    [
        START_DATE,
        END_DATE,
    ]
)
NT_errors_filename = namedtuple(
    'NT_errors',
    [
        'error_file_origin',
        'error_task_origin',
    ]
)
NT_error = namedtuple(
    'error',
    [
        'error',
    ]
)

# GLOBAL VARIABLES
ERROR_FOLDER: str = EMPTY_STRING
ERROR_LIST: List[str] = []
ERROR_FILE_ORIGIN: str = EMPTY_STRING
ERROR_TASK_ORIGIN: str = EMPTY_STRING
PARTITION_GROUP: int = 1
PARTITION_TOTAL: int = 1


def generate_date_pair_for_full_series(
        list_date_pairs: List[Tuple[str, str]],
) -> tuple:
    first_end_date: str
    last_start_date: str

    first_start_date: str
    last_end_date: str
    first_start_date, first_end_date = list_date_pairs[0]
    last_start_date, last_end_date = list_date_pairs[-1]

    return NT_date_pair(start_date=first_start_date, end_date=last_end_date)


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
) -> str:
    return numeric.replace(TIME_SHIFT_NEGATIVE_SYMBOL, HYPHEN)


def generate_filename(
        filename_nt: tuple,
        delimiter: str = EMPTY_STRING,
        folder: str = EMPTY_STRING,
        extension: str = EMPTY_STRING,
) -> str:
    output: str = delimiter.join(filename_nt)
    if folder:
        output = folder + output
    if extension:
        output = output + extension
    return output


def parse_filename(
        filename: str,
        delimiter: str,
        named_tuple,  # NamedTuple annotation doesn't work when using it as a callable
        extension: str = EMPTY_STRING,
) -> tuple:
    if extension:
        filename = filename.replace(extension, EMPTY_STRING)
    split_filename: List[str] = filename.split(delimiter)
    parsed_filename: NamedTuple
    try:
        parsed_filename = named_tuple(*split_filename)
    except TypeError:
        error: str = f'parse_filename{HYPHEN}named_tuple_size_mismatch{HYPHEN}filename'
        parsed_filename = NT_error(error)
        log_error(error=error)

    return parsed_filename


def aggregate_data_in_folder(
        folder_input: str,
        folder_output_aggregate: str,
        optional_filename_label: str = EMPTY_STRING,
        list_cities: Tuple[str, ...] = tuple(DEFAULT_CITIES),
        bool_suppress_print: bool = True,
) -> None:
    generate_sub_paths_for_folder(
        folder=folder_output_aggregate,
    )
    if optional_filename_label:
        filename_label: str = f'{HYPHEN}{optional_filename_label}'
    else:
        filename_label = EMPTY_STRING
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
                    f'{folder_input}{filename}',
                )
                list_parsed_dfs_per_city.append(df)
            except pd.io.common.EmptyDataError:
                log_error(
                    error=f'{city}{HYPHEN}{AGGREGATE}{HYPHEN}{filename}{HYPHEN}{ERROR_EMPTY}',
                    bool_suppress_print=bool_suppress_print,
                )
        if len(list_parsed_dfs_per_city) > 0:
            log_error(error=f'{AGGREGATE} : {city}', log=True)
            concatenated_data_per_city: pd.DataFrame = pd.concat(
                list_parsed_dfs_per_city,
                sort=True,
            )
            concatenated_data_per_city.to_csv(
                f'{folder_output_aggregate}{city}{HYPHEN}{filename_label}{CSV}',
                index=False,
            )
            list_data_dfs_for_all_cities.append(concatenated_data_per_city)
        else:
            log_error(error=f'{city}{HYPHEN}{ERROR_EMPTY}')
        write_errors_to_disk(
            clear_task_origin=False,
            bool_suppress_print=bool_suppress_print,
            overwrite=False,
        )

    df_aggregate: pd.DataFrame = pd.concat(
        list_data_dfs_for_all_cities,
        sort=True,
    )
    df_aggregate.to_csv(
        f'{folder_output_aggregate}{AGGREGATE}{filename_label}{CSV}',
        index=False,
    )


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
    assert (partition_group > 0), f'The partition group must equal 1 or more'
    assert partition_group <= partition_total, f'Partition group: {partition_group} exceeds partition total {partition_total}.'
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
    with open(f'{filename}') as json_file:
        json_data = json.load(json_file)
        return json_data


def import_single_file(
        folder: str,
        list_filename_filter_conditions: Tuple[str, ...] = DEFAULT_AGGREGATE_FILENAME_FILTER_CONDITIONS,
) -> str:
    filename: str
    list_rest: List[str]
    list_filenames: List[str] = import_paths_from_folder(
        folder=folder,
        list_paths_filter_conditions=list_filename_filter_conditions,
    )
    if len(list_filenames) > 0:
        filename, *list_rest = list_filenames
        if len(list_rest) != 0:
            print(
                f'Multiple filename candidates; using the first candidate: {filename}. These are the rest: {list_rest}.'
            )
            filename_extra_candidate: str
            for filename_extra_candidate in list_rest:
                log_error(error=f'parse_filename{HYPHEN}extra_candidate{HYPHEN}{filename_extra_candidate}')

        return filename

    else:
        log_error(error=f'parse_filename{HYPHEN}no_candidate')

        return EMPTY_STRING


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
                    if os.path.isfile(f'{folder}{path}')
                ]
            if include_folders:
                list_paths = [
                    path
                    for path in list_paths
                    if os.path.isdir(f'{folder}{path}')
                ]
            if ignore_hidden:
                pattern_hidden_path: Pattern[str] = re.compile(r'\.[A-Za-z]')
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
    directories: List[str] = folder.split('/')
    recursive_sub_directories: Iterator[str] = itertools.accumulate(directories, lambda x, y: '/'.join([x, y]))
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
    output_filename: str = f'{folder}{filename}'
    output_write_type: str
    if os.path.exists(output_filename):
        output_write_type = 'a+'
    else:
        output_write_type = 'w+'
        generate_sub_paths_for_folder(
            folder=folder,
        )

    with open(output_filename, output_write_type) as output_file:
        for string in list_strings:
            if string not in output_file:
                output_file.write(str(string))
                output_file.write('\n')
    output_file.close()


def check_partition_valid_for_aggregation(
        error_label: str,
        partition_group: int,
        partition_total: int,
) -> bool:
    if get_partition_group() > 1 or get_partition_total() > 1:
        log_error(error=f'{ERROR_PARTITION}{HYPHEN}{error_label}')
        print_statement: str = f'Can only aggregate {error_label} when run as a SINGLE partition group ' \
                               f'AND with ONE total partition due to race conditions. ' \
                               f'Not guaranteed that the other partitions have finished running; please check and rerun. ' \
                               f'Current partition group: {partition_group}. ' \
                               f'Current partition total: {partition_total}.'
        print(print_statement)
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
    assert partition_total > 0, f'Partition total must be above 0'
    PARTITION_TOTAL = partition_total
    assert PARTITION_TOTAL >= PARTITION_GROUP, f'Partition total must be greater than or equal to the partition groups'


def set_partition_group(
        partition_group: int = 1,
) -> None:
    global PARTITION_GROUP
    assert partition_group > 0, f'Partition group must be above 0'
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
            print(f'{LOG} : {error}')
        else:
            print(f'error : {error}')


def set_error_folder(
        error_folder: str,
) -> None:
    global ERROR_FOLDER
    ERROR_FOLDER = error_folder
    print(f'error_folder : {error_folder}')


def set_error_file_origin(
        file_origin: str,
) -> None:
    global ERROR_FILE_ORIGIN
    ERROR_FILE_ORIGIN = file_origin
    print(f'file_origin : {file_origin}')


def set_error_task_origin(
        task_origin: str,
) -> None:
    global ERROR_TASK_ORIGIN
    ERROR_TASK_ORIGIN = task_origin
    if task_origin:
        print(f'task_origin : {task_origin}')


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
        print(f'No file origin was specified for the error log.')
    if ERROR_TASK_ORIGIN:
        error_task_origin = ERROR_TASK_ORIGIN
    else:
        error_task_origin = UNKNOWN
        print(f'No task origin was specified for the error log.')

    output_filename: str = generate_filename(
        filename_nt=NT_errors_filename(
            error_file_origin=error_file_origin,
            error_task_origin=error_task_origin,
        ),
        delimiter=HYPHEN,
        folder=folder_error,
        extension=TXT,
    )
    output_write_type: str
    if overwrite:
        output_write_type = 'w+'
    elif os.path.exists(output_filename):
        output_write_type = 'a+'
    else:
        output_write_type = 'w+'

    with open(output_filename, output_write_type) as error_log_file:
        current_datetime: str = datetime.now().strftime('%m/%d/%Y %H:%M:%S')
        error_log_file.write(f'{current_datetime} : {error_task_origin}')
        error_log_file.write('\n')

        error: str
        for error in ERROR_LIST:
            error_log_file.write(str(error))
            error_log_file.write('\n')
        error_log_file.write('\n')
        error_log_file.write('\n')

    ERROR_LIST = []
    if clear_task_origin:
        set_error_task_origin(EMPTY_STRING)
    if not bool_suppress_print:
        print(f'Error log outputted to: {output_filename}')


if __name__ == '__main__':
    print(f'universal.py is not meant to be run as a script. Aborting.')
    exit(1)

with open(f'{UNIVERSAL}{HYPHEN}{PARAMETERS}{JSON}') as universal_json_file:
    universal_parameters = json.load(universal_json_file)
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
    FOLDER_KEYWORDS: str = universal_parameters[PARAM_FOLDER_KEYWORDS]
    FOLDER_STATS_INTERCITY_RAW: str = universal_parameters[PARAM_FOLDER_STATS_INTERCITY_RAW]
    FOLDER_STATS_INTERCITY_AGGREGATE: str = universal_parameters[PARAM_FOLDER_STATS_INTERCITY_AGGREGATE]
    FOLDER_TRENDS_RAW: str = universal_parameters[PARAM_FOLDER_TRENDS_RAW]
    FOLDER_TRENDS_STITCH: str = universal_parameters[PARAM_FOLDER_TRENDS_STITCH]
    FOLDER_TRENDS_AGGREGATE: str = universal_parameters[PARAM_FOLDER_TRENDS_AGGREGATE]
    FOLDER_TRENDS_METRICS_RAW: str = universal_parameters[PARAM_FOLDER_TRENDS_METRICS_RAW]
    FOLDER_TRENDS_METRICS_AGGREGATE: str = universal_parameters[PARAM_FOLDER_TRENDS_METRICS_AGGREGATE]
universal_json_file.close()
