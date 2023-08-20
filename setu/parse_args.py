import argparse

def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def add_commmon_args(parser):

    parser.add_argument(
        "--config",
        type=str,
        required=True,
        help="Path to config file",
    )

    parser.add_argument(
        "--samples_per_partition",
        type=int,
        default=1500,
        required=False,
        help="No.of samples per partition",
    )

    parser.add_argument(
        "--verbose",
        type=str2bool,
        default=False,
        required=False,
        help="Whether to add `show()` at each stage.",
    )

    parser.add_argument(
        "--checkpoint_dir",
        type=str,
        required=False,
        default=None,
        help="Path to the folder which will store checkpoints",
    )

    return parser

def add_analysis_args(parser):

    parser.add_argument(
        "--run_analysis",
        type=str2bool,
        required=False,
        default=False,
        help="Whether to run analysis",
    )

    parser.add_argument(
        "--df_parquets_path",
        type=str,
        required=False,
        default=None,
        help="Path to folder containing parquets",
    )

    parser.add_argument(
        "--is_df_path_batched",
        type=str2bool,
        required=False,
        default=True,
        help="Is path a batch path or not?",
    )

    parser.add_argument(
        "--save_doc_lid_output",
        type=str2bool,
        default=True,
        required=False,
        help="Whether to store lid checkpoint",
    )

    parser.add_argument(
        "--doc_lid_output_path",
        type=str,
        required=False,
        default=None,
        help="Path of the folder store lid checkpoint",
    )

    parser.add_argument(
        "--save_line_stats_output",
        type=str2bool,
        default=True,
        required=False,
        help="Whether to store line stats checkpoint",
    )

    parser.add_argument(
        "--line_stats_output_path",
        type=str,
        required=False,
        default=None,
        help="Path of the folder store line stats checkpoint",
    )

    parser.add_argument(
        "--save_doc_stats_output",
        type=str2bool,
        default=True,
        required=False,
        help="Whether to store doc stats checkpoint",
    )

    parser.add_argument(
        "--doc_stats_output_path",
        type=str,
        required=False,
        default=None,
        help="Path of the folder store doc stats checkpoint",
    )

    parser.add_argument(
        "--analysis_output_path",
        type=str,
        required=False,
        default=None,
        help="Path to the folder to store analysis output",
    )

    return parser

def add_plotting_args(parser):

    # TODO: Add plotting args
    
    return parser

def add_filtering_args(parser):

    parser.add_argument(
        "--run_flag_and_filter",
        type=str2bool,
        required=False,
        default=False,
        help="Whether to run flagging and filtering",
    )

    parser.add_argument(
        "--doc_stats_parquets_path",
        type=str,
        required=False,
        default=None,
        help="Path to folder containing parquets",
    )

    parser.add_argument(
        "--is_doc_stats_path_batched",
        type=str2bool,
        required=False,
        default=True,
        help="Is path a batch path or not?",
    )

    parser.add_argument(
        "--save_nsfw_data",
        type=str2bool,
        default=True,
        required=False,
        help="Whether to store nsfw data",
    )

    parser.add_argument(
        "--nsfw_output_path",
        type=str,
        required=False,
        default=None,
        help="Path of the folder to store nsfw data",
    )

    parser.add_argument(
        "--filtered_doc_stats_output_path",
        type=str,
        required=False,
        default=None,
        help="Path to the folder to store fitered output",
    )

    return parser

def add_doc_removal_args(parser):

    parser.add_argument(
        "--remove_documents",
        type=str2bool,
        required=False,
        default=False,
        help="Whether to remove documents or not",
    )

    parser.add_argument(
        "--filtered_docs_path",
        type=str,
        required=False,
        default=None,
        help="Path to the folder to store fitered output",
    )

    return parser

def parse_args():

    parser = argparse.ArgumentParser(description="Runs Setu for Analysis/Plotting/Flagging/Filtering")

    parser = add_commmon_args(parser)

    parser = add_analysis_args(parser)

    parser = add_plotting_args(parser)

    parser = add_filtering_args(parser)

    parser = add_doc_removal_args(parser)
    
    args = parser.parse_args()

    return args
