# stats_input_handler.py

#need to change from input to argpase for UI friendly entering


def get_stat_choice(logger):
    """
    Ask user to choose which performance stat they want to analyze.
    Returns the selected option as a string.
    """

    options = [
        "NFS Portal Stats Averaged over 60 secs",
        "SMB Portal Stats Averaged over 60 secs",
        "S3 Portal Stats Averaged over 60 secs",
        "View Stats Averaged over 60 secs",
        "Blob Data Journal Stats Averaged over 60 secs",
        "Journal stats Averaged over 60 secs",
        "Metadata Journal stats Averaged over 60 secs",
        "Disk Controller Stats Averaged over 60 secs",
        "Disk Stats Averaged over 60 secs",
        "Icebox Vault Node Stats Averaged over 60 secs",
    ]
    options.append("Exit")

    choice=4
    return options[choice-1]

    print("\nPlease choose a performance stat table to analyze:\n")
    print("0. Exit")
    for idx, name in enumerate(options, start=1):
        print(f"{idx}. {name}")

    while True:
        choice = input("\nEnter the number of your choice: ").strip()

        if choice == "0":
            if logger:
                logger.info("User chose to exit the program.")
            print("Exiting...")
            exit(0)

        if choice.isdigit():
            choice = int(choice)
            if 1 <= choice <= len(options)-1:  # last option is exit
                logger.info(f"User option choosen : {options[choice - 1]}")
                return options[choice - 1]
            
        print("Invalid input. Please enter a number between 0 and", len(options)-1)
        logger.error("Invalid input")


def NFS_Portal_Stats_Averaged_over_60_secs(logger):
    return

def SMB_Portal_Stats_Averaged_over_60_secs(logger):
    return

def s3_Portal_Stats_Averaged_over_60_secs(logger):
    return

def view_Portal_Stats_Averaged_over_60_secs(logger):
    return "10907017373:TestAndDev:6"

def get_input_for_selected_stat(option, logger):
    """
    Receive the selected stat option and ask for additional input if needed.
    Example: if option == "NFS Portal Stats Averaged over 60 secs",
             ask for server name or view name.
    """
    if option == "NFS Portal Stats Averaged over 60 secs": return NFS_Portal_Stats_Averaged_over_60_secs
    if option == "S3 Portal Stats Averaged over 60 secs": return s3_Portal_Stats_Averaged_over_60_secs(logger)
    elif option == "SMB Portal Stats Averaged over 60 secs": return SMB_Portal_Stats_Averaged_over_60_secs(logger)
    elif option == "View Stats Averaged over 60 secs": return view_Portal_Stats_Averaged_over_60_secs(logger)
    
    else:
        logger.info(f"No additional input required for: {option}")
        return None

