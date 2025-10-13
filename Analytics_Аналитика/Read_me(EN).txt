Data_DWH_update.ipynb - Example of one of the analyst’s scripts responsible for updating and maintaining the DWH data mart.
                        This script is used to refresh the database containing information about blocked and low-performing partners.

                        Before loading new data, it checks existing records: each partner in the DWH can have only one non-zero status.
                        If such a status already exists, the script assigns a new numeric flag, classifying statuses according
                        to the priority rule 4 > 1 > 3 > 2 > 0.
                        At the same time, a source can have any number of zero flags, since they represent only historical data.

                        After validating all necessary conditions and adding “scam” sources from a separate Google Sheet,
                        the script overwrites the data mart in the DWH and sends a notification about successful completion
                        to a dedicated Telegram channel.
                        It also reports whether any flag changes occurred after loading the new data and indicates their count.
