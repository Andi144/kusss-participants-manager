import argparse
import os.path
import re

import pandas as pd

COURSE_ID_COL = "course_id"
DEFAULT_OUTPUT_FILE = "merged_participants.csv"

parser = argparse.ArgumentParser()
parser.add_argument("kusss_participants_files", type=str, nargs="+",
                    help="KUSSS participants CSV export files. To ensure that the file contents are correctly matched "
                         "to the corresponding course, the files must either include the course ID number (e.g., "
                         "365.123 or 365123) anywhere in their filenames, or the argument 'course_ids' must be "
                         "specified for each of the CSV files (same order). All of the CSV files must have the same "
                         "format (the same columns in the same order).")
parser.add_argument("--course_ids", type=str, nargs="+", default=None,
                    help="If specified, the corresponding course ID (e.g., 365.123 or 365123) for each of the CSV "
                         "files specified in 'kusss_participants_files'. Both lists must have exactly the same number "
                         "of elements, and their order is exactly as specified in the arguments. If this argument is "
                         "set, then the CSV filenames are not parsed for course IDs, i.e., the explicitly given course "
                         "IDs take precedence. Default: None, i.e., the course IDs are tried to be extracted from the "
                         "CSV filenames.")
parser.add_argument("-mc", "--merge_cols", type=str, nargs="+", default=["Matrikelnummer"],
                    help="The CSV columns to use for merging all CSV files. Default: ['Matrikelnummer']")
parser.add_argument("-e", "--encoding", type=str, default="ANSI",
                    help="The character encoding of the CSV files. Default: 'ANSI'")
parser.add_argument("-s", "--separator", type=str, default=";",
                    help="The separator character of the CSV files. Default: ';'")
parser.add_argument("-o", "--output_file", type=str, default=None,
                    help=f"The path of the output CSV file. By default (None), a file called '{DEFAULT_OUTPUT_FILE}' "
                         "will be created in the same directory as the first CSV file from 'kusss_participants_files'. "
                         "The output CSV will be encoded in UTF-8. Default: None")
parser.add_argument("-sc", "--sort_cols", type=str, nargs="+", default=["Matrikelnummer"],
                    help="The CSV columns to use for sorting the output CSV. Default: ['Matrikelnummer']")
args = parser.parse_args()

course_ids = args.course_ids
if course_ids is None:
    course_ids = []
    for file in args.kusss_participants_files:
        match = re.search(r"\d{6}|\d{3}\.\d{3}", os.path.basename(file))
        if match is None:
            raise ValueError(f"file '{file}' does not contain a valid course ID")
        else:
            course_ids.append(match.group())
    assert len(course_ids) == len(args.kusss_participants_files)
elif len(course_ids) != len(args.kusss_participants_files):
    raise ValueError("'kusss_participants_files' and 'course_ids' must have exactly the same number of elements")

dfs = []
for file, course_id in zip(args.kusss_participants_files, course_ids):
    df = pd.read_csv(file, encoding=args.encoding, sep=args.separator, dtype=str)
    df[COURSE_ID_COL] = course_id
    dfs.append(df)

all_df = pd.concat(dfs)
dup = all_df.duplicated(subset=args.merge_cols, keep=False)
single_df = all_df[~dup]
multiple_df = all_df[dup]
assert len(single_df) + len(multiple_df) == len(all_df)

rows = []
# Avoid pd.groupby warning when number of merge columns is 1
for _, group_df in multiple_df.groupby(args.merge_cols if len(args.merge_cols) > 1 else args.merge_cols[0]):
    row = group_df.iloc[0]  # Just grab any, they should all be identical (except for the course ID)
    row[COURSE_ID_COL] = sorted(group_df[COURSE_ID_COL].tolist())
    rows.append(row)

merged_df = pd.concat([single_df.copy(), pd.DataFrame(rows)])
assert not merged_df.duplicated(subset=args.merge_cols).any()
merged_df = merged_df.sort_values(by=args.sort_cols)

output_file = args.output_file
if output_file is None:
    output_file = os.path.join(os.path.dirname(args.kusss_participants_files[0]), DEFAULT_OUTPUT_FILE)
merged_df.to_csv(output_file, index=False, encoding="utf-8")
