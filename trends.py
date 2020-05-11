import random
import sys
import time
from datetime import timedelta

import numpy as np
from pytrends.exceptions import ResponseError
from pytrends.request import TrendReq

from universal import *

# STATIC VARIABLES
ERROR_MAX_VOLUME: str = 'max_volume_exceeds'
ERROR_RESPONSE: str = 'response_missing'
IS_PARTIAL: str = 'isPartial'
US: str = 'US'

# DEFAULT
DEFAULT_DOWNLOAD_WITH_COMMON: bool = False
DEFAULT_ONLY_STITCH_MISSING: bool = True
DEFAULT_VALIDATE_DOWNLOAD: bool = False
DEFAULT_IS_RELATIVE_TO_COMMON_WORD: bool = False
DEFAULT_SPLIT_TRENDS_RAW_FILENAME_LENGTH: int = 5
DEFAULT_SOURCE_FOLDERS: Tuple[str, ...] = (
    "cgap",
)

# PARAMETERS
PARAM_VALIDATE_DOWNLOAD: str = 'validate_download'
PARAM_DOWNLOAD_WITH_COMMON_WORD: str = 'download_with_common_word'
PARAM_ONLY_STITCH_MISSING: str = 'only_stitch_missing'
PARAM_SOURCE_FOLDERS_TO_DOWNLOAD: str = 'source_folders_to_download'

# NAMED TUPLES
NT_trends_raw_filename = namedtuple(
    'NT_filename_trends_raw',
    [
        CITY,
        KEYWORD,
        COMMON_WORD,
        START_DATE,
        END_DATE,
    ]
)
NT_trends_stitch_filename = namedtuple(
    'NT_filename_trends_stitch',
    [
        CITY,
        KEYWORD,
        COMMON_WORD,
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
    set_error_file_origin(TRENDS)
    set_error_folder(FOLDER_ERROR)
    set_partition_group(partition_group)
    set_partition_total(partition_total)
    with open(f'{PARAMETERS}{HYPHEN}{TRENDS}{JSON}') as json_file:
        json_data = json.load(json_file)
        aggregate: bool
        download: bool
        validate_download: bool
        stitch: bool
        only_stitch_missing: bool
        list_partitioned_cities: Tuple[str, ...]
        list_source_folders_to_download: List[str] = json_data[PARAM_SOURCE_FOLDERS_TO_DOWNLOAD]
        if called_from_main:
            download = json_data[DOWNLOAD]
            validate_download = json_data[PARAM_VALIDATE_DOWNLOAD]
            stitch = json_data[STITCH]
            only_stitch_missing = json_data[PARAM_ONLY_STITCH_MISSING]
            aggregate = json_data[AGGREGATE]
            parameters: dict = json_data[TRENDS]
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
            download = False
            validate_download = False
            stitch = True
            only_stitch_missing = True
            aggregate = False
            list_partitioned_cities = list_cities
        bool_download_with_common_word: bool = json_data[PARAM_DOWNLOAD_WITH_COMMON_WORD]
    json_file.close()

    list_date_pairs: List[Tuple[str, str]] = list(zip(START_DATES, END_DATES))
    start_date: str
    end_date: str
    start_date, end_date = generate_date_pairs(list_date_pairs)

    if download:
        set_error_task_origin(task_origin=DOWNLOAD)
        city: str
        for city in list_partitioned_cities:
            log_error(f'{DOWNLOAD} : {city}', log=True)
            dict_keyword_sets: Dict[str, List[Tuple[str, str]]] = generate_keywords_to_download_dict(
                city=city,
                list_date_pairs=list_date_pairs,
                list_source_folders_to_download=tuple(list_source_folders_to_download),
                folder_keywords=FOLDER_KEYWORDS,
                folder_trends_raw=FOLDER_TRENDS_RAW,
                common_word=COMMON_WORD_UNIVERSAL,
                bool_download_with_common_word=bool_download_with_common_word,
                validate_download=validate_download,
            )
            if dict_keyword_sets:
                submit_dma_based_query(
                    city=city,
                    dict_keyword_set=dict_keyword_sets,
                    common_word=COMMON_WORD_UNIVERSAL,
                    bool_download_with_common_word=bool_download_with_common_word,
                    folder_trends_raw=FOLDER_TRENDS_RAW,
                )
            write_errors_to_disk(overwrite=False)
        print(f'Finished downloading keywords for partition group: {get_partition_group()} of {get_partition_total()}.')

    if stitch:
        set_error_task_origin(task_origin=STITCH)
        for city in list_partitioned_cities:
            stitch_trends_raw_for_city(
                city=city,
                start_date=start_date,
                end_date=end_date,
                list_date_pairs=list_date_pairs,
                common_word=COMMON_WORD_UNIVERSAL,
                folder_trends_raw=FOLDER_TRENDS_RAW,
                folder_trends_stitch=FOLDER_TRENDS_STITCH,
                max_volume=MAX_SEARCH_VOLUME,
                only_stitch_missing=only_stitch_missing,
            )
            write_errors_to_disk(overwrite=(not only_stitch_missing))

    if aggregate:
        set_error_task_origin(task_origin=AGGREGATE)
        partitioning_is_valid_for_aggregation: bool = check_partition_valid_for_aggregation(
            error_label=TRENDS,
            partition_group=get_partition_group(),
            partition_total=get_partition_total(),
        )
        if partitioning_is_valid_for_aggregation:
            df_aggregate_keywords: pd.DataFrame = aggregate_trends_stitched(
                start_date=start_date,
                end_date=end_date,
                list_cities=tuple(list_input_cities),
                list_source_folders_to_download=tuple(list_source_folders_to_download),
                folder_keywords=FOLDER_KEYWORDS,
                folder_trends_stitch=FOLDER_TRENDS_STITCH,
                folder_trends_aggregate=FOLDER_TRENDS_AGGREGATE,
            )
            output_trends_aggregate_filename: str = generate_filename(
                filename_nt=NT_aggregate_filename(
                    aggregate=AGGREGATE,
                ),
                extension=CSV,
            )
            df_aggregate_keywords.to_csv(
                f'{FOLDER_TRENDS_AGGREGATE}{output_trends_aggregate_filename}',
                index=True,
            )
        write_errors_to_disk()


def generate_keywords_to_download_dict(
        city: str,
        list_date_pairs: List[Tuple[str, str]],
        folder_keywords: str = FOLDER_KEYWORDS,
        folder_trends_raw: str = FOLDER_TRENDS_RAW,
        common_word: str = DEFAULT_COMMON_WORD,
        list_source_folders_to_download: Tuple[str, ...] = DEFAULT_SOURCE_FOLDERS,
        bool_download_with_common_word: bool = DEFAULT_DOWNLOAD_WITH_COMMON,
        validate_download: bool = DEFAULT_VALIDATE_DOWNLOAD,
) -> Dict[str, List[Tuple[str, str]]]:
    dict_keywords: dict = generate_keywords(
        folder_keywords=folder_keywords,
    )

    source: str
    list_all_keywords: List[str] = []
    for source in list_source_folders_to_download:
        if dict_keywords.get(source, EMPTY_STRING):
            list_all_keywords.extend(dict_keywords[source].keys())

    list_all_keywords = list(set(list_all_keywords))
    list_already_downloaded_filenames: List[str] = import_paths_from_folder(
        folder=folder_trends_raw,
        list_paths_filter_conditions=(city,),
    )

    dict_keyword_sets: dict = {}

    def add_keyword_to_keyword_sets(
            keyword: str,
            start_date: str,
            end_date: str,
    ):
        if not dict_keyword_sets.get(keyword, []):
            dict_keyword_sets.update({keyword: []})
        dict_keyword_sets[keyword].append((start_date, end_date))

    start_date: str
    end_date: str
    for keyword in list_all_keywords:
        for start_date, end_date in list_date_pairs:
            nt_expected_trends_raw_filename: tuple = NT_trends_raw_filename(
                city=city,
                keyword=keyword,
                common_word=generate_common_word_filename_output(
                    keyword=keyword,
                    common_word=common_word,
                    bool_download_with_common_word=bool_download_with_common_word,
                ),
                start_date=generate_date_for_filename_output(
                    date=start_date,
                ),
                end_date=generate_date_for_filename_output(
                    date=end_date,
                ),
            )
            expected_filename: str = generate_filename(
                filename_nt=nt_expected_trends_raw_filename,
                delimiter=HYPHEN,
                extension=CSV,
            )
            download_is_missing_dates: bool = False
            if validate_download:
                if expected_filename in list_already_downloaded_filenames:
                    download_is_missing_dates = is_download_missing_dates(
                        filename=expected_filename,
                        folder_trends_raw=folder_trends_raw,
                        start_date=start_date,
                        end_date=end_date,
                    )
            if download_is_missing_dates:
                add_keyword_to_keyword_sets(
                    keyword=keyword,
                    start_date=start_date,
                    end_date=end_date,
                )
                log_error(error=f'VALIDATE{HYPHEN}{expected_filename}')
            else:
                if expected_filename not in list_already_downloaded_filenames:
                    add_keyword_to_keyword_sets(
                        keyword=keyword,
                        start_date=start_date,
                        end_date=end_date,
                    )

    return dict_keyword_sets


def generate_common_word_filename_output(
        keyword: str,
        common_word: str,
        bool_download_with_common_word: bool,
) -> str:
    if bool_download_with_common_word:
        return common_word
    else:
        return keyword


def generate_keywords(
        folder_keywords: str = FOLDER_KEYWORDS,
) -> dict:
    sub_folder: str
    keyword: str
    filename: str
    dict_keywords: dict = {}
    for sub_folder in import_paths_from_folder(
            folder_keywords,
            check_paths=True,
            include_files=False,
            include_folders=True,
    ):
        source: str = sub_folder
        list_keywords: List[str] = []
        for filename in import_paths_from_folder(
                folder=f'{folder_keywords}{sub_folder}',
                list_paths_filter_conditions=(TXT,),
        ):
            keyword_file = open(f'{folder_keywords}{sub_folder}{FORWARD_SLASH}{filename}', 'r')
            list_keywords.extend(
                [
                    keyword.lower().strip()
                    for keyword in keyword_file
                ]
            )
            keyword_file.close()
        dict_source_keywords: dict = {
            keyword: source
            for keyword in list_keywords
        }
        dict_keywords.update({source: dict_source_keywords})

    return dict_keywords


def is_download_missing_dates(
        filename: str,
        folder_trends_raw: str,
        start_date: str,
        end_date: str,
) -> bool:
    df: pd.DataFrame = pd.read_csv(f'{folder_trends_raw}{filename}')
    if df.empty:
        return False
    else:
        parsed_start_date: str = df[DATE].iloc[0]
        parsed_end_date: str = df[DATE].iloc[-1]
        return not ((parsed_start_date == start_date) and (parsed_end_date == end_date))


def submit_dma_based_query(
        city: str,
        dict_keyword_set: Dict[str, List[Tuple[str, str]]],
        common_word: str = DEFAULT_COMMON_WORD,
        bool_download_with_common_word: bool = DEFAULT_DOWNLOAD_WITH_COMMON,
        folder_trends_raw: str = FOLDER_TRENDS_RAW,
) -> None:
    generate_sub_paths_for_folder(
        folder=folder_trends_raw,
    )
    pytrend = TrendReq()
    geo_code: str
    if city == USA:
        geo_code = US
    else:
        geo_code = f'{US}{HYPHEN}{str(DEFAULT_CITIES[city][STATE_NAME])}{HYPHEN}{str(DEFAULT_CITIES[city][DMA])}'
    print(f'GEO Code : {geo_code}')

    keyword: str
    list_of_date_pairs: List[Tuple[str, str]]
    for keyword, list_of_date_pairs in dict_keyword_set.items():
        if bool_download_with_common_word:
            if common_word in keyword:
                continue
            else:
                kw_set = [common_word, keyword]
                print(f'{city} : {keyword} : {common_word}')
        else:
            kw_set = [keyword]
            print(f'{city} : {keyword}')

        start_date: str
        end_date: str
        pair_of_dates: Tuple[str, str]
        for pair_of_dates in list_of_date_pairs:
            start_date, end_date = pair_of_dates
            start_date = start_date.replace(UNDERSCORE, HYPHEN)
            end_date = end_date.replace(UNDERSCORE, HYPHEN)
            nt_trends_raw_filename: tuple = NT_trends_raw_filename(
                city=city,
                keyword=keyword,
                common_word=generate_common_word_filename_output(
                    keyword=keyword,
                    common_word=common_word,
                    bool_download_with_common_word=bool_download_with_common_word,
                ),
                start_date=generate_date_for_filename_output(
                    date=start_date,
                ),
                end_date=generate_date_for_filename_output(
                    date=end_date,
                ),
            )
            output_nt_trends_raw_filename: str = generate_filename(
                filename_nt=nt_trends_raw_filename,
                delimiter=HYPHEN,
                extension=CSV,
            )

            tm: str = f'{start_date}{SINGLE_SPACE}{end_date}'
            time.sleep(random.randrange(2, 5))
            try:
                pytrend.build_payload(
                    kw_list=kw_set,
                    geo=geo_code,
                    timeframe=tm,
                )
            except ResponseError:
                log_error(error=f'{ERROR_RESPONSE}{HYPHEN}{output_nt_trends_raw_filename}')
                return

            df_trend_interest: pd.DataFrame = pytrend.interest_over_time()
            if df_trend_interest.empty:
                pd.DataFrame().to_csv(f'{folder_trends_raw}{output_nt_trends_raw_filename}')
                log_error(error=f'{ERROR_EMPTY}{HYPHEN}{output_nt_trends_raw_filename}')
            else:
                df_trend_interest.to_csv(f'{folder_trends_raw}{output_nt_trends_raw_filename}')


def stitch_trends_raw_for_city(
        city: str,
        start_date: str,
        end_date: str,
        list_date_pairs: List[Tuple[str, str]],
        common_word: str = DEFAULT_COMMON_WORD,
        folder_trends_raw: str = FOLDER_TRENDS_RAW,
        folder_trends_stitch: str = FOLDER_TRENDS_STITCH,
        max_volume: float = MAX_SEARCH_VOLUME,
        only_stitch_missing: bool = DEFAULT_ONLY_STITCH_MISSING,
) -> None:
    log_error(f'{STITCH} : {city}', log=True)
    generate_sub_paths_for_folder(
        folder=folder_trends_stitch,
    )

    dict_keywords_file_paths: Dict[str, Dict[str, List[str]]] = generate_trends_raw_file_paths_dict(
        city=city,
        folder_trends_raw=folder_trends_raw,
    )

    df_keyword_common_global: pd.DataFrame = generate_df_from_trends_raw_file_paths(
        city=city,
        common_word=common_word,
        keyword=common_word,
        dict_keywords_file_paths=dict_keywords_file_paths,
        folder_trends_raw=folder_trends_raw,
        list_date_pairs=list_date_pairs,
    )
    error_global_common_word: str
    df_keyword_common_global, error_global_common_word = stitch_and_clean_keyword_df(
        keyword=common_word,
        df_keyword=df_keyword_common_global,
        start_date=start_date,
        end_date=end_date,
        keyword_frequency_label=COMMON_WORD_FREQUENCY,
        bool_is_relative_to_common_word=False,
        common_word=common_word,
        max_search_frequency=max_volume,
    )

    if error_global_common_word:
        log_error(error=f'{city}{HYPHEN}{common_word}{HYPHEN}{error_global_common_word}')

    list_already_stitched_trends_filenames: List[str] = import_paths_from_folder(
        folder_trends_stitch,
        (city, CSV),
    )

    keyword: str
    for keyword, dict_file_paths_for_keyword in dict_keywords_file_paths.items():
        parsed_common_word: str
        nt_trends_stitch_filename = NT_trends_stitch_filename(
            city=city,
            keyword=keyword,
            common_word=common_word,
            start_date=generate_date_for_filename_output(
                date=start_date,
            ),
            end_date=generate_date_for_filename_output(
                date=end_date,
            ),
        )
        output_trends_stitch_filename: str = generate_filename(
            filename_nt=nt_trends_stitch_filename,
            delimiter=HYPHEN,
            extension=CSV,
        )

        if not only_stitch_missing or output_trends_stitch_filename not in list_already_stitched_trends_filenames:
            print(f'{STITCH} : {city} : {keyword}')

            error_keyword: str
            df_keyword: pd.DataFrame = generate_df_from_trends_raw_file_paths(
                city=city,
                common_word=keyword,
                keyword=keyword,
                dict_keywords_file_paths=dict_keywords_file_paths,
                folder_trends_raw=folder_trends_raw,
                list_date_pairs=list_date_pairs,
            )
            df_keyword, error_keyword = stitch_and_clean_keyword_df(
                keyword=keyword,
                df_keyword=df_keyword,
                start_date=start_date,
                end_date=end_date,
                bool_is_relative_to_common_word=False,
                common_word=common_word,
                max_search_frequency=max_volume,
            )
            if error_keyword:
                log_error(error=f'{city}{HYPHEN}{keyword}{HYPHEN}{error_keyword}')

            error_keyword_common: str
            df_keyword_common: pd.DataFrame = generate_df_from_trends_raw_file_paths(
                city=city,
                common_word=common_word,
                keyword=keyword,
                dict_keywords_file_paths=dict_keywords_file_paths,
                folder_trends_raw=folder_trends_raw,
                list_date_pairs=list_date_pairs,
            )

            df_keyword_common, error_keyword_common = stitch_and_clean_keyword_df(
                keyword=keyword,
                df_keyword=df_keyword_common,
                start_date=start_date,
                end_date=end_date,
                bool_is_relative_to_common_word=True,
                common_word=common_word,
            )
            if error_keyword_common:
                log_error(error=f'{city}{HYPHEN}{keyword}{HYPHEN}{error_keyword_common}')

            df: pd.DataFrame = pd.concat(
                [
                    df_keyword,
                    df_keyword_common,
                    df_keyword_common_global,
                ],
                join='outer',
                axis=1,
                sort=True,
            )

            df.to_csv(
                f'{folder_trends_stitch}{output_trends_stitch_filename}',
                index=True,
                index_label=DATE,
                date_format=DATE_FORMAT,
            )


def generate_trends_raw_file_paths_dict(
        city: str,
        folder_trends_raw: str = FOLDER_TRENDS_RAW,
) -> Dict[str, Dict[str, List[str]]]:
    dict_filenames: Dict[str, Dict[str, List[str]]] = {}

    filename: str
    keyword: str
    parsed_city: str
    parsed_common_word: str
    start_date: str
    end_date: str
    for filename in import_paths_from_folder(
            folder=folder_trends_raw,
            list_paths_filter_conditions=(CSV, city),
    ):
        nt_parsed_trends_raw_filename = parse_filename(
            filename=filename,
            delimiter=HYPHEN,
            named_tuple=NT_trends_raw_filename,
            extension=CSV,
        )

        try:
            # noinspection PyStatementEffect
            nt_parsed_trends_raw_filename.error
        except AttributeError:
            pass
        else:
            log_error(error=f'critical_error{HYPHEN}parse_trends_raw_filename{HYPHEN}{filename}')
            continue

        if nt_parsed_trends_raw_filename.city != city:
            log_error(error=f'city_mismatch{HYPHEN}{nt_parsed_trends_raw_filename.city}')
            continue

        keyword_dict: Dict[str, List[str]] = dict_filenames.get(nt_parsed_trends_raw_filename.keyword, {})
        if not keyword_dict:
            dict_filenames.update({nt_parsed_trends_raw_filename.keyword: {}})

        list_file_paths: List[str] = keyword_dict.get(nt_parsed_trends_raw_filename.common_word, [])
        if not list_file_paths:
            dict_filenames[nt_parsed_trends_raw_filename.keyword].update(
                {nt_parsed_trends_raw_filename.common_word: []})
        dict_filenames[nt_parsed_trends_raw_filename.keyword][nt_parsed_trends_raw_filename.common_word].append(
            filename)

    return dict_filenames


def generate_df_from_trends_raw_file_paths(
        city: str,
        common_word: str,
        keyword: str,
        dict_keywords_file_paths: Dict[str, Dict[str, List[str]]],
        folder_trends_raw: str,
        list_date_pairs: List[Tuple[str, str]],
) -> pd.DataFrame:
    dict_file_paths_for_keyword: Dict[str, List[str]] = dict_keywords_file_paths.get(keyword, {})
    list_common_word_filenames: List[str] = dict_file_paths_for_keyword.get(common_word, [])
    if not list_common_word_filenames:
        return pd.DataFrame()

    list_common_word_filenames.sort()
    list_common_word_dfs: List[pd.DataFrame] = []
    list_parsed_date_pairs: List[Tuple[str, str]] = []

    for common_word_filename in list_common_word_filenames:
        nt_parsed_common_word_filename = parse_filename(
            filename=common_word_filename,
            delimiter=HYPHEN,
            extension=CSV,
            named_tuple=NT_trends_raw_filename,
        )
        start_date: str = parse_filename_date(nt_parsed_common_word_filename.start_date)
        end_date: str = parse_filename_date(nt_parsed_common_word_filename.end_date)
        if nt_parsed_common_word_filename.city != city:
            log_error(error=f'city_mismatch{HYPHEN}{common_word_filename}')
        if keyword != nt_parsed_common_word_filename.keyword:
            log_error(error=f'keyword_mismatch{HYPHEN}{common_word_filename}')

        df: pd.DataFrame = pd.read_csv(f'{folder_trends_raw}{common_word_filename}')
        if df.empty:
            df = generate_empty_time_series_df(
                start_date=start_date,
                end_date=end_date,
            )
            df[common_word] = 0
        if IS_PARTIAL in df.columns:
            df.drop(columns=IS_PARTIAL, inplace=True)

        list_common_word_dfs.append(df)
        list_parsed_date_pairs.append((start_date, end_date))

    list_missing_date_pairs: List[Tuple[str, str]] = [
        date_pair
        for date_pair in list_date_pairs
        if date_pair not in list_parsed_date_pairs
    ]

    for date_pair in list_missing_date_pairs:
        log_error(error=f'missing_date_pair{HYPHEN}{city}{HYPHEN}{keyword}{HYPHEN}{common_word}{HYPHEN}{date_pair}')

    return pd.concat(
        objs=list_common_word_dfs,
        ignore_index=True,
    )


def stitch_and_clean_keyword_df(
        keyword: str,
        df_keyword: pd.DataFrame,
        start_date: str,
        end_date: str,
        keyword_frequency_label: str = KEYWORD_FREQUENCY,
        bool_is_relative_to_common_word: bool = DEFAULT_IS_RELATIVE_TO_COMMON_WORD,
        common_word: str = DEFAULT_COMMON_WORD,
        max_search_frequency: float = MAX_SEARCH_VOLUME,
) -> Tuple[pd.DataFrame, str]:
    df_empty: pd.DataFrame = generate_empty_time_series_df(
        start_date=start_date,
        end_date=end_date,
    )
    df_empty.set_index(DATE, inplace=True)
    error: str
    df_keyword_filled_time_region: pd.DataFrame
    if not bool_is_relative_to_common_word:
        if df_keyword.empty:
            error = ERROR_EMPTY
            return df_empty, error

        else:
            df_keyword, error = stitch_keyword_df(
                df_keyword=df_keyword,
                max_search_frequency=max_search_frequency,
            )
            df_keyword.rename(columns={keyword: keyword_frequency_label}, inplace=True)
            df_keyword_filled_time_region = pd.merge(
                df_empty,
                df_keyword,
                how='left',
                left_index=True,
                right_index=True,
            )
            return df_keyword_filled_time_region, error

    else:
        label_common_word_frequency_relative: str = COMMON_WORD_FREQUENCY_RELATIVE
        label_keyword_frequency_relative: str = KEYWORD_FREQUENCY_RELATIVE
        if df_keyword.empty:
            df_empty[label_common_word_frequency_relative] = np.NAN
            df_empty[label_keyword_frequency_relative] = np.NAN
            error = ERROR_EMPTY
            return df_empty, error

        else:
            df_keyword, error = stitch_keyword_df(
                df_keyword=df_keyword,
                max_search_frequency=max_search_frequency,
            )
            df_keyword.rename(
                columns={
                    common_word: label_common_word_frequency_relative
                },
                inplace=True,
            )
            if keyword == common_word:
                df_keyword[label_keyword_frequency_relative] = df_keyword
            else:
                df_keyword.rename(
                    columns={
                        keyword: label_keyword_frequency_relative,
                    },
                    inplace=True,
                )

            df_keyword_filled_time_region = pd.merge(
                df_empty,
                df_keyword,
                how='left',
                left_index=True,
                right_index=True,
            )
            return df_keyword_filled_time_region, error


def stitch_keyword_df(
        df_keyword: pd.DataFrame,
        max_search_frequency: float = MAX_SEARCH_VOLUME,
        date_format: str = DATE_FORMAT,
) -> Tuple[pd.DataFrame, str]:
    columns: List[str] = list(df_keyword.columns)
    column: str
    for column in columns:
        if column == DATE:
            continue
        trend_max_value: float = df_keyword[column].max()
        if trend_max_value > max_search_frequency:
            return pd.DataFrame(), ERROR_MAX_VOLUME

    first_date_in_table: pd.DataFrame = df_keyword[DATE].iloc[0]
    last_date_in_table: pd.DataFrame = df_keyword[DATE].iloc[-1]
    months_in_table: pd.DatetimeIndex = pd.date_range(
        start=first_date_in_table,
        end=last_date_in_table,
        freq='M',
    )

    list_of_stitch_time_ranges: List[pd.DataFrame] = []
    is_first_run: bool = True
    scale: float = 1.0
    first_slice_index = None

    last_datetime_in_month: datetime
    for last_datetime_in_month in months_in_table:
        first_day_in_month: str = datetime(last_datetime_in_month.year, last_datetime_in_month.month, 1).strftime(
            date_format)
        last_day_in_month: str = last_datetime_in_month.strftime(date_format)
        duplicate_first_day_in_month_list = np.where(df_keyword[DATE] == first_day_in_month)[0]
        duplicate_last_day_in_month_list = np.where(df_keyword[DATE] == last_day_in_month)[0]

        is_stitching_month: bool = len(duplicate_first_day_in_month_list) > 1 and len(
            duplicate_last_day_in_month_list) > 1
        is_last_month: bool = last_day_in_month == months_in_table[-1].strftime(date_format)

        if is_first_run and first_slice_index is None:
            first_slice_index = duplicate_first_day_in_month_list[0]
            continue

        if is_stitching_month or is_last_month:
            df_time_range: pd.DataFrame
            if is_stitching_month:
                df_time_range = df_keyword.iloc[first_slice_index: duplicate_last_day_in_month_list[0] + 1]
            elif is_last_month:
                df_time_range = df_keyword.iloc[first_slice_index:]
            else:
                return pd.DataFrame(), f'stitch_keyword{HYPHEN}both_stitching_month_and_last_month_are_false'
            df_time_range.set_index(DATE, inplace=True)

            if not is_first_run:
                df_time_range = df_time_range.apply(lambda x: x * scale)
            else:
                is_first_run = False
            list_of_stitch_time_ranges.append(df_time_range)

            if is_last_month:
                break

            past_average: pd.DataFrame = df_keyword.iloc[
                                         duplicate_first_day_in_month_list[0]: duplicate_last_day_in_month_list[0] + 1
                                         ].replace(0, pd.np.NaN).mean(axis=0)
            future_average: pd.DataFrame = df_keyword.iloc[
                                           duplicate_first_day_in_month_list[1]: duplicate_last_day_in_month_list[1] + 1
                                           ].replace(0, pd.np.NaN).mean(axis=0)
            past: float = max(1.0, past_average.iloc[0])
            future: float = max(1.0, future_average.iloc[0])
            scale = 1.0  # future_avg / past_avg
            first_slice_index = duplicate_last_day_in_month_list[1] + 1

    if len(list_of_stitch_time_ranges) > 0:

        return (
            pd.concat(
                list_of_stitch_time_ranges,
                sort=True,
            ),
            EMPTY_STRING,
        )

    else:

        return pd.DataFrame(), ERROR_EMPTY


def generate_empty_time_series_df(
        start_date: str,
        end_date: str,
        date_format: str = DATE_FORMAT,
) -> pd.DataFrame:
    s_dt: datetime = datetime.strptime(start_date, date_format)
    e_dt: datetime = datetime.strptime(end_date, date_format)
    delta: timedelta = e_dt - s_dt
    dates_list: List[str] = [
        (s_dt + timedelta(days=x)).strftime(date_format)
        for x in range(delta.days + 1)
    ]

    return pd.DataFrame({DATE: dates_list})


def aggregate_trends_stitched(
        start_date: str,
        end_date: str,
        list_cities: Tuple[str, ...] = tuple(DEFAULT_CITIES),
        list_source_folders_to_download: Tuple[str, ...] = (),
        folder_keywords: str = FOLDER_KEYWORDS,
        folder_trends_stitch: str = FOLDER_TRENDS_STITCH,
        folder_trends_aggregate: str = FOLDER_TRENDS_AGGREGATE,
) -> pd.DataFrame:
    generate_sub_paths_for_folder(
        folder=folder_trends_aggregate,
    )
    list_all_stitched_cities_dfs: List[pd.DataFrame] = []
    dict_keywords: dict = generate_keywords(
        folder_keywords=folder_keywords,
    )
    source_dict: Dict[str, str]
    source_error: str
    source_dict, source_error = generate_source_dict_from_keywords_dict(
        dict_keywords=dict_keywords,
        list_source_folders_to_download=list_source_folders_to_download,
    )
    city: str
    for city in list_cities:
        log_error(error=f'{AGGREGATE} : {city}', log=True)

        list_stitched_keywords_for_city_dfs: List[pd.DataFrame] = []
        stitched_keyword_filename: str
        for stitched_keyword_filename in import_paths_from_folder(
                folder=folder_trends_stitch,
                list_paths_filter_conditions=(CSV, city),
        ):
            nt_parsed_trends_stitch_filename = parse_filename(
                filename=stitched_keyword_filename,
                delimiter=HYPHEN,
                named_tuple=NT_trends_stitch_filename,
                extension=CSV,
            )

            if nt_parsed_trends_stitch_filename.city != city:
                log_error(
                    error=f'city_mismatch{HYPHEN}{city}{HYPHEN}{nt_parsed_trends_stitch_filename.city}{HYPHEN}{stitched_keyword_filename}')
                continue
            if nt_parsed_trends_stitch_filename.start_date != generate_date_for_filename_output(start_date):
                log_error(error=f'start_date_mismatch{HYPHEN}{start_date}{HYPHEN}{stitched_keyword_filename}')
            if nt_parsed_trends_stitch_filename.end_date != generate_date_for_filename_output(end_date):
                log_error(error=f'end_date_mismatch{HYPHEN}{end_date}{HYPHEN}{stitched_keyword_filename}')
            df_stitched_keyword: pd.DataFrame = pd.read_csv(
                f'{folder_trends_stitch}{stitched_keyword_filename}',
                parse_dates=[DATE],
                infer_datetime_format=True,
                index_col=DATE,
            )
            df_stitched_keyword.insert(0, CITY, city)
            df_stitched_keyword.insert(1, COMMON_WORD, nt_parsed_trends_stitch_filename.common_word)
            df_stitched_keyword.insert(2, KEYWORD, nt_parsed_trends_stitch_filename.keyword)
            source: str = source_dict.get(nt_parsed_trends_stitch_filename.keyword, source_error)
            if source == source_error:
                log_error(error=f'{city}{HYPHEN}{UNKNOWN}{HYPHEN}{SOURCE}{HYPHEN}{nt_parsed_trends_stitch_filename.keyword}')
            df_stitched_keyword.insert(3, SOURCE, source)
            list_stitched_keywords_for_city_dfs.append(df_stitched_keyword)

        df_city_stitch_keywords: pd.DataFrame = pd.concat(
            list_stitched_keywords_for_city_dfs,
            sort=True,
        )
        output_aggregate_for_city_filename: str = generate_filename(
            filename_nt=NT_city_aggregate_filename(
                city=city,
            ),
            extension=CSV,
        )
        df_city_stitch_keywords.to_csv(
            f'{folder_trends_aggregate}{output_aggregate_for_city_filename}',
            index_label=DATE,
        )
        list_all_stitched_cities_dfs.append(df_city_stitch_keywords)
        write_errors_to_disk(
            clear_task_origin=False,
            overwrite=False,
        )

    return pd.concat(
        list_all_stitched_cities_dfs,
        sort=True,
    )


def generate_source_dict_from_keywords_dict(
        dict_keywords: dict,
        list_source_folders_to_download: Tuple[str, ...] = (),
) -> (Dict[str, str], str):
    keyword_error: str
    if len(dict_keywords) == 0:
        log_error(
            error=f'{MISSING}{HYPHEN}dict_keywords',
            bool_suppress_print=True,
        )
        return {}, MISSING
    else:
        dict_source: Dict[str, str] = {}
        source_folder: str
        dict_source.update(
            {
                keyword: source_folder
                for source_folder in reversed(list_source_folders_to_download)
                for keyword in dict_keywords.get(source_folder, {})
            }
        )
        return dict_source, UNKNOWN


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
