## os_data_pipeline

This project provides an open-source data pipeline for extracting live and historic electricity generation data from Elexon's Balancing Mechanism Reporting Service (BMRS) API: using the "Elexon Data Portal" package, historic & reconciled generation data (up to ca 5 working days in the past) is extracted from the "B1610" report ("Actual Generation Output Per Generation Unit"). To fill the gaps to the most recent data, as operational metering available to National Grid is not published, additional near real-time data is extracted from the BMRS "Physical Data" report, namely:
* (Final) Physical Notifications (FPN): the information each generator shares with Elexon & National Grid about what they expect to generate in any half-hourly settlement period (SP). FPNs are submitted at "Gate Closure" for each SP, i.e. one hour prior to the start of each SP. For conventional generators (e.g. gas or nuclear) this is a commercial position determined by how much electricity they have contracted to sell for each settlement period, and usually fairly closely matches the generator's actual output.
* Bid-Offer Acceptance Level (BOAL or BOALF): In order to balance the grid, electricity generators may have to adjust their generation at short notice. These adjustments are represented in the form of bid-offer acceptance data. Bid-offer acceptances are often shorter than an entire SP, and multiple BOALs can exist for the same SP, i.e. if an earlier BOAL is overridden later on in the SP. The biggest effort of determining the "live" generation stems from translating and summarising potentially multiple BOAL records per SP and generator into a dataset that represents what the generator was meant to export during each SP.
* Maximum Export Level (MEL): MEL is the maximum that the generator could output if asked to do so. It is determined by physical limitations on which parts of the generator are operating at the time: for example, parts of a generator could be on outage, limiting its overall output.  MEL can be changed at any time by the generator, and, in the event of a breakdown, generators are required to reset their MELs to the new value as soon as practicable. In effect, FPNs capped by MELs are the best estimate of what a conventional generator is doing in real time, in the absence of Operational Metering data.
* The official definitions of these datasets can be found here: https://www.bmreports.com/bmrs/?q=help/glossary
<br>
All BMRS data is queried using the Elexon Data Portal package (https://github.com/OSUKED/ElexonDataPortal).
<br>
In order to provide location data alongside the generation data, locations of wind farms have been mapped using the data available from the "Power Station Dictionary" GitHub repository (https://github.com/OSUKED/Power-Station-Dictionary).
<br>
<br>

## Limitations to be aware of

By design, this data pipeline will only capture data about electricity generators which are sending data to the BMRMs, namely larger generators which export into the UK's transmission network. This means that a lot of smaller generators (e.g. small onshore wind farms) and embedded generation (e.g. rooftop solar) will not be included in this dataset. Likewise, electricity flow via interconnectors has not been included in this pipeline.
<br>
As part of this project, we performed considerable reconciliation between the historic (B1610) and the physical BMRS data to understand the limitations of the proposed approach. This highlighted the following limitations:
* For intermittent generation, such as wind, FPNs are a lot less accurate as they rely on forecasts. The data quality of submitted FPNs varies considerably for different wind farms, with some generators simply submitting FPNs that match their installed capacity.
* Sheffield Solar, in collaboration with ESO, do publish live estimates of generation, based on a combination of live metering from domestic and small/medium solar farms, and live weather data of solar radiation round the country. This data is not currently integrated in the live-generation map.

## Future Development Ideas
1. Replace FPNs for wind farms with worst forecast performance with an improved wind forecast.
2. Integrate data from Sheffield Solar.


## Configuration


### Git Hooks

These are scripts that are run automatically before and/or after certain git commands. 
This is done to improve consistency in version control for the project across commits and different contributors.  
After cloning this repository for the first time, you must setup git to use the git_hooks. To do this, from the project directory run:


    git config --local core.hooksPath git_hooks/


For more information on what the git-hooks do, see the scripts themselves.  

### Environment Variables

We use an environment variable to set the parent directory where the data for the project is stored. If you have not done so already you will need to configure this. On windows go to the start menu and find "edit environment variables for your account", from here you should be able to add an environment variable with the parameters:  

The pipeline will require two environment variables to be set up:
1. OSDP: The "data" folders will be created in this directory with the downloaded data from the Balancing Mechanism Reporting Service (BMRS). This could be the location of this repo on your machine. 
2. BMRS_API_KEY: this should be the API key you received when registering on the elexonportal.co.uk website. You will need this in order to query the BMRS API.


### Requirements  
* jupytext - Install on your machine using "pip install jupytext"  
* conda working from the command line - Run "conda --version" to check you have this set up (should get something like "conda 4.12.0")
        If not, you need to add conda to your path environment variable  
            To do this: open your anaconda prompt and run "where conda": You should get some paths to .bat and .exe files  
            Add these paths (minus the file name at the end) to the path environment variable (and restart your terminal)  
            You should now have access to conda from the command line.  
* black working with jupyter notebooks: "pip install black" and "pip install black[jupyter]"  

