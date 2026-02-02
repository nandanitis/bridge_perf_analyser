import sys
from utils.utils import STAT_MAP_FOR_ARGPARSE,STAT_OPTIONS,parse_args

#choice=4
#return options[choice-1]

def get_stat_choice_manual(logger):
    """Take stat input interactively from user."""
    SUFFIX = "Averaged over 60 secs"

    print("\nPlease choose a performance stat to analyze:\n")
    print("0. Exit")
    for idx, name in STAT_OPTIONS.items():
        print(f"{idx}. {name}")

    while True:
        choice = input("\nEnter the stat number: ").strip()

        if choice == "0":
            logger.info("User chose to exit the program.")
            print("Exiting...")
            sys.exit(0)

        if choice.isdigit():
            choice = int(choice)
            if choice in STAT_OPTIONS:
                selected = f"{STAT_OPTIONS[choice]} {SUFFIX}"
                logger.info("User option chosen: %s", selected)
                return selected

        print("Invalid input. Please enter a valid option.")
        logger.error("Invalid input for stat selection")


def get_stat_choice(logger):
    """
    First try argparse input.
    If not provided, fall back to manual selection.
    """
    args = parse_args()

    if args.stat:
        selected_stat = STAT_MAP_FOR_ARGPARSE.get(args.stat.lower())

        if not selected_stat:
            logger.error("Invalid stat argument: %s", args.stat)
            print(f"Invalid stat argument: {args.stat}")
            sys.exit(1)

        logger.info("Stat selected via argparse: %s", selected_stat)
        return selected_stat

    return get_stat_choice_manual(logger)


def NFS_Portal_Stats_Averaged_over_60_secs(stat_identifier,logger):
    return

def SMB_Portal_Stats_Averaged_over_60_secs(stat_identifier,logger):
    return

def s3_Portal_Stats_Averaged_over_60_secs(stat_identifier,logger):
    return

def view_Portal_Stats_Averaged_over_60_secs(stat_identifier,logger):
    print(stat_identifier)
    return "10907017373:TestAndDev:6"

def get_input_for_selected_stat(option, logger):
    """
    Receive the selected stat option and ask for additional input if needed.
    Example: if option == "NFS Portal Stats Averaged over 60 secs",
             ask for server name or view name.
    """
    stat_identifier=None
    args = parse_args()
    if args.id:
        stat_identifier = args.id
    if option == "NFS Portal Stats Averaged over 60 secs": return NFS_Portal_Stats_Averaged_over_60_secs(stat_identifier,logger)
    if option == "S3 Portal Stats Averaged over 60 secs": return s3_Portal_Stats_Averaged_over_60_secs(stat_identifier,logger)
    elif option == "SMB Portal Stats Averaged over 60 secs": return SMB_Portal_Stats_Averaged_over_60_secs(stat_identifier,logger)
    elif option == "View Stats Averaged over 60 secs": return view_Portal_Stats_Averaged_over_60_secs(stat_identifier,logger)
    
    else:
        logger.info(f"No additional input required for: {option}")
        return None

