import sys
from utils.utils import STAT_MAP_FOR_ARGPARSE,STAT_OPTIONS,parse_args


def get_input_for_selected_stat(selected_stat, logger):
    
    """We are inputing the Name/Id to filter for from the selected stats"""
    stat_identifier=None
    args = parse_args()
    if args.id:
        stat_identifier = args.id
        return stat_identifier
    return "10907017373:TestAndDev:6"
    stat_identifier=input("\n Please Enter the Name/Id you want to check from the  {selected_stat} to analyze: \n")
    return stat_identifier


def get_stat_choice_manual(logger):
    """Take stat input interactively from user."""
    SUFFIX = "Averaged over 60 secs"

    print("\nPlease choose a performance stat to analyze:\n")
    print("0. Exit")
    for idx, name in STAT_OPTIONS.items():
        print(f"{idx}. {name}")
    print("10. Only display Admission Controller Stats and QOS Details ")

    while True:
        choice = input("\nEnter the stat number: ").strip()

        if choice == "0":
            logger.info("User chose to exit the program.")
            print("Exiting...")
            sys.exit(0)
        
        if choice == "10":
            return "Bridge Stats Only"

        if choice.isdigit():
            choice = int(choice)
            if choice in STAT_OPTIONS:
                selected = f"{STAT_OPTIONS[choice]} {SUFFIX}"
                return selected

        print("Invalid input. Please enter a valid option.")
        logger.error("Invalid input for stat selection")


def get_stat_choice(logger):
    return "View Stats Averaged over 60 secs"
    """First try argparse input.If not provided, fall back to manual selection."""
    args = parse_args()

    if args.stat:
        selected_stat = STAT_MAP_FOR_ARGPARSE.get(args.stat.lower())

        if not selected_stat:
            logger.error("Invalid stat argument: %s", args.stat)
            logger.info(f"Invalid stat argument: {args.stat}")
            sys.exit(1)

        logger.info("Stat selected via argparse: %s", selected_stat)
        return selected_stat

    return get_stat_choice_manual(logger)


def collect_run_configuration(logger):
     
    """
    Collects user inputs for the stats analyser run.
    - Stat type (NFS or SMB or View or Bridge-only, etc.)
    - Optional stat identifier (View ID or client IP or bucket name or etc.)

    Returns:
        selected_stat (str)
        stat_identifier (str | None)
        is_bridge_only (bool)
    """
    selected_stat=get_stat_choice(logger)
    logger.debug(f"User has opted stats analyser for  : {selected_stat}")

    stat_identifier=None

    is_bridge_only = (selected_stat == "Bridge Stats Only")
    if not is_bridge_only:
        stat_identifier = get_input_for_selected_stat(selected_stat, logger)
        logger.debug(f"User opted analyser for {selected_stat}, Name/Id: {stat_identifier}")

    return selected_stat,stat_identifier,is_bridge_only