# cfl-data-model

<h3>Yearly Data Steps:</h3>

1. Run the school_spider to make sure we add a new row for any new FBS schools that have been added.
2. Run the school_year_stats_spider to add the team details to the school_year_stats table for the required years (team SoS, etc.)
3. Run player_spider for the required year(s). This will add a new row for any FBS players that the DB cannot find from previous years. Then it will add a new row for each specified year for each player in FBS. This is the spider that handles inputting player's yearly statistics into our DB.
4. Go to PFF and download the rushing and receiving CSV reports for the necessary years. Add these reports to the correct /data folders and follow the existing naming convention.
5. Run the pff_spider for the required year(s) to update the players' stat rows with the PFF-related fields.

Note ^ These steps can all be run BEFORE the NFL combine and draft. After the combine and draft are complete, run the following steps:

6. Go to ras.football and download the CSV reports for WR, RB and TE from the needed draft year(s). Add these reports to the correct /data folders and follow the existing naming convention.
7. Run the ras_spider - this parses through the CSV files we have in our /data folder and updates the player rows with their RAS score from the combine.
8. Run the draft_spider - this parses through all draft selections for the specified year(s) and updates the player rows with the necessary data (draft year, draft pick, height/weight, birthday, etc.)