"""
Author: Mohammad Dehghani Ashkezari <mdehghan@uw.edu>

Date: 2020-05-07

Function: Encapsulates CMAP RESTful API calls.
"""

using HTTP 
using CSV
using DataFrames
using JSON



mutable struct API
    token :: String
    domain :: String
    tokenPrefix :: String
    
    function API(
                token::String="", 
                tokenPrefix::String="Api-Key ", 
                domain::String="https://simonscmap.com"
                )

        if isempty(token)
            token = get_api_key();
        end            

        @assert(length(token)>0, "API key not found!")                

        api = new();
        api.token = token;
        api.tokenPrefix = tokenPrefix;
        api.domain = domain;
        api
    end
end



function api_key_fname()
    return "api_key.csv"
end


"""
    set_api_key(key::String) 

Saves your API key on local disk.

Examples
≡≡≡≡≡≡≡≡≡≡
set_api_key("Your API Key") 

"""
function set_api_key(key::String)    
    CSV.write(api_key_fname(), DataFrame(apiKey = key))
end    


"""
    get_api_key()

Returns your api key, if exists.

Examples
≡≡≡≡≡≡≡≡≡≡
get_api_key()

"""
function get_api_key()
    keyFile = api_key_fname()
    if ~isfile(keyFile)
        error("API Key file not found. The following command will register the API key:\n\nset_api_key(key::String)\n\n")
    end    
    return DataFrame(CSV.File(keyFile))[1, 1]
end


function atomic_request(api::API, route, payload)
    try
        baseURL = api.domain;        
        tokenPrefix = api.tokenPrefix;        
        token = tokenPrefix * api.token;
        queryString = "";
        if ~isempty(payload)
            queryString = HTTP.escapeuri(payload);
        end            
        url = baseURL * route * queryString;        
        headers = (("Authorization", token), 
                   ("content-type","application/json")
                  )
        response = HTTP.request("GET", url, headers);
        status = response.status;
        respStr = String(response.body);
        df = CSV.read(IOBuffer(chomp(respStr)))
        #CSV.write("./dump.csv", df)
        return status, df
    catch e
        println("Error in atomic_request : $e")
        return 
    end
end



"""
    query(api::API, queryStatement::String)

Takes a custom SQL query statement and returns the results in form of a dataframe.

Examples
≡≡≡≡≡≡≡≡≡≡
api = API()
status, response = query(api, "SELECT * FROM tblSensors")
println(response)

"""
function query(api::API, queryStatement::String)
    payload = Dict("query" => queryStatement);
    route = "/api/data/query?";
    return atomic_request(api, route, payload)
end    



"""
    get_catalog()

Returns a dataframe containing full Simons CMAP catalog of variables.

Examples
≡≡≡≡≡≡≡≡≡≡
get_catalog()

"""
function get_catalog()
    api = API();
    status, response = query(api, "EXEC uspCatalog");
    return response
end    



"""
    search_catalog(keywords::String)

Returns a dataframe containing a subset of Simons CMAP catalog of variables. 
All variables at Simons CMAP catalog are annotated with a collection of semantically related keywords. 
This method takes the passed keywords and returns all of the variables annotated with similar keywords.
The passed keywords should be separated by blank space. The search result is not sensitive to the order of keywords and is not case sensitive.
The passed keywords can provide any 'hint' associated with the target variables. Below are a few examples: 

* the exact variable name (e.g. NO3), or its linguistic term (Nitrate)

* methodology (model, satellite ...), instrument (CTD, seaflow), or disciplines (physics, biology ...) 

* the cruise official name (e.g. KOK1606), or unofficial cruise name (Falkor)

* the name of data producer (e.g Penny Chisholm) or institution name (MIT)

If you searched for a variable with semantically-related-keywords and did not get the correct results, please let us know. 
We can update the keywords at any point.

Examples
≡≡≡≡≡≡≡≡≡≡
search_catalog("nitrite falkor")

"""
function search_catalog(keywords::String)
    api = API();
    status, response = query(api, "EXEC uspSearchCatalog '$keywords'");
    return response
end   



"""
    datasets()

Returns a dataframe containing the list of data sets hosted by Simons CMAP database.

Examples
≡≡≡≡≡≡≡≡≡≡
datasets()

"""
function datasets()
    api = API();
    status, response = query(api, "EXEC uspDatasets");
    return response
end 


"""
    head(tableName::String, rows::Integer=5)

Returns top records of a data set.

Examples
≡≡≡≡≡≡≡≡≡≡
head("tblFalkor_2018")

"""
function head(tableName::String, rows::Integer=5)
    api = API();
    status, response = query(api, "EXEC uspHead '$tableName', $rows" );
    return response
end 


"""
    columns(tableName::String)

Returns the list of data set columns.

Examples
≡≡≡≡≡≡≡≡≡≡
columns("tblAMT13_Chisholm")

"""
function columns(tableName::String)
    api = API();
    status, response = query(api, "EXEC uspColumns '$tableName'" );
    return response
end 



"""
    get_dataset_ID(tableName::String)

Returns dataset ID.

Examples
≡≡≡≡≡≡≡≡≡≡
get_dataset_ID("tblCHL_REP")

"""
function get_dataset_ID(tableName::String)
    api = API();
    status, response = query(api, "SELECT DISTINCT(Dataset_ID) FROM dbo.udfCatalog() WHERE LOWER(Table_Name)=LOWER('$tableName') " );
    if nrow(response) < 1
        error("Invalid table name: $tableName")
    end    
    if nrow(response) > 1
        error("More than one table found. Please provide a more specific name: ")
        println(response);
    end    

    return response.Dataset_ID[1]
end 


"""
    get_dataset(tableName::String)

Returns the entire dataset.
It is not recommended to retrieve datasets with more than 100k rows using this method.
For large datasets, please use the 'space_time' method and retrieve the data in smaller chunks.
Note that this method does not return the dataset metadata. 
Use the 'get_dataset_metadata' method to get the dataset metadata.

Examples
≡≡≡≡≡≡≡≡≡≡
get_dataset("tblHOT_LAVA")

"""
function get_dataset(tableName::String)
    datasetID = get_dataset_ID(tableName);
    maxRow = 2000000;
    api = API();
    status, response = query(api, "SELECT JSON_stats FROM tblDataset_Stats WHERE Dataset_ID=$datasetID");
    jDict = JSON.parse(response.JSON_stats[1]);
    rows = parse(Int, jDict["lat"]["count"]);
    if rows > maxRow
        msg = "\nThe requested dataset has $rows records.\n";
        msg *= "It is not recommended to retrieve datasets with more than $maxRow rows using this method.\n";
        msg *= "For large datasets, please use the 'space_time' method and retrieve the data in smaller chunks.\n"; 
        error(msg);
    end 
    status, response = query(api, "SELECT * FROM $tableName")
    return response
end 


"""
    get_dataset_metadata(tableName::String)

Returns a dataframe containing the dataset metadata.

Examples
≡≡≡≡≡≡≡≡≡≡
get_dataset_metadata("tblArgoMerge_REP")

"""
function get_dataset_metadata(tableName::String)
    api = API();
    status, response = query(api, "EXEC uspDatasetMetadata '$tableName'" );
    return response
end 


"""
    get_var(tableName::String, varName::String)

Returns a single-row dataframe from tblVariables containing info associated with varName.
This method is mean to be used internally and will not be exposed at documentations.

Examples
≡≡≡≡≡≡≡≡≡≡
get_var("tblCHL_REP", "chl")

"""
function get_var(tableName::String, varName::String)
    api = API();
    status, response = query(api, "SELECT * FROM tblVariables WHERE Table_Name='$tableName' AND Short_Name='$varName'" );
    return response
end 


"""
    get_var_catalog(tableName::String, varName::String)

Returns a single-row dataframe from catalog (udfCatalog) containing all of the variable's info at catalog.

Examples
≡≡≡≡≡≡≡≡≡≡
get_var_catalog("tblCHL_REP", "chl")

"""
function get_var_catalog(tableName::String, varName::String)
    api = API();
    status, response = query(api, "SELECT * FROM [dbo].udfCatalog() WHERE Table_Name='$tableName' AND Variable='$varName'" );
    return response
end 


"""
    get_var_long_name(tableName::String, varName::String)

Returns the long name of a given variable.

Examples
≡≡≡≡≡≡≡≡≡≡
get_var_long_name("tblAltimetry_REP", "adt")

"""
function get_var_long_name(tableName::String, varName::String)
    api = API();
    status, response = query(api, "SELECT Long_Name, Short_Name FROM tblVariables WHERE Table_Name='$tableName' AND  Short_Name='$varName'");
    return response.Long_Name[1]
end 


"""
    get_unit(tableName::String, varName::String)

Returns the long name of a given variable.

Examples
≡≡≡≡≡≡≡≡≡≡
get_unit("tblHOT_ParticleFlux", "silica_hot")

"""
function get_unit(tableName::String, varName::String)
    api = API();
    status, response = query(api, "SELECT Unit, Short_Name FROM tblVariables WHERE Table_Name='$tableName' AND  Short_Name='$varName'");
    return response.Unit[1]
end 


"""
    get_var_resolution(tableName::String, varName::String)

Returns the long name of a given variable.

Examples
≡≡≡≡≡≡≡≡≡≡
get_var_resolution("tblModis_AOD_REP", "AOD")

"""
function get_var_resolution(tableName::String, varName::String)
    api = API();
    status, response = query(api, "EXEC uspVariableResolution '$tableName', '$varName'");
    return response
end 



"""
    get_var_coverage(tableName::String, varName::String)

Returns a single-row dataframe from catalog (udfCatalog) containing the variable's spatial and temporal coverage.

Examples
≡≡≡≡≡≡≡≡≡≡
get_var_coverage("tblCHL_REP", "chl")

"""
function get_var_coverage(tableName::String, varName::String)
    api = API();
    status, response = query(api, "EXEC uspVariableCoverage '$tableName', '$varName'");
    return response
end 



"""
    get_var_stat(tableName::String, varName::String)

Returns a single-row dataframe from catalog (udfCatalog) containing the variable's summary statistics.

Examples
≡≡≡≡≡≡≡≡≡≡
get_var_stat("tblHOT_LAVA", "Prochlorococcus")

"""
function get_var_stat(tableName::String, varName::String)
    api = API();
    status, response = query(api, "EXEC uspVariableStat '$tableName', '$varName'");
    return response
end 



"""
    has_field(tableName::String, varName::String)

Returns a single-row dataframe from catalog (udfCatalog) containing the variable's summary statistics.

Examples
≡≡≡≡≡≡≡≡≡≡
has_field("tblAltimetry_REP", "sla")

"""
function has_field(tableName::String, varName::String)
    api = API();
    status, response = query(api, "SELECT COL_LENGTH('$tableName', '$varName') AS RESULT");
    if isequal(response.RESULT[1], missing)
        return false
    end    
    return true
end 



"""
    is_grid(tableName::String, varName::String)

Returns a boolean indicating whether the variable is a gridded product or has irregular spatial resolution.

Examples
≡≡≡≡≡≡≡≡≡≡
is_grid("tblArgoMerge_REP", "argo_merge_salinity_adj")

"""
function is_grid(tableName::String, varName::String)
    api = API();
    grid = true;
    statement = "SELECT Spatial_Res_ID, RTRIM(LTRIM(Spatial_Resolution)) AS Spatial_Resolution FROM tblVariables "
    statement *= "JOIN tblSpatial_Resolutions ON [tblVariables].Spatial_Res_ID=[tblSpatial_Resolutions].ID "
    statement *= "WHERE Table_Name='$tableName' AND Short_Name='$varName' "

    status, response = query(api, statement);
    if nrow(response) < 1
        return missing
    end    
    if strip(lowercase(response.Spatial_Resolution[1])) == lowercase("irregular")
        return false    
    end    
    return true    
end 


"""
    is_climatology(tableName::String)

Returns true if the table represents a climatological data set.    
Currently, the logic is based on the table name.
TODO: Ultimately, it should query the DB to determine if it's a climatological data set.

Examples
≡≡≡≡≡≡≡≡≡≡
is_climatology("tblDarwin_Plankton_Climatology")

"""
function is_climatology(tableName::String)
    occursin("_Climatology", tableName) ? true : false
end    



"""
    get_references(datasetID::Int)

Returns a dataframe containing refrences associated with a data set.

Examples
≡≡≡≡≡≡≡≡≡≡
get_references(21)

"""
function get_references(datasetID::Int)
    api = API();
    status, response = query(api, "SELECT Reference FROM dbo.udfDatasetReferences($datasetID)");
    return response
end   



"""
    get_metadata(tableName::String, varName::String)

Returns a dataframe containing the associated metadata.

Examples
≡≡≡≡≡≡≡≡≡≡
get_metadata("tblsst_AVHRR_OI_NRT", "sst")

"""
function get_metadata(tableName::String, varName::String)
    api = API();
    status, response = query(api, "EXEC uspVariableMetaData '$tableName', '$varName'");
    return response
end 



"""
    cruises()

Returns a dataframe containing a list of all of the hosted cruise names.

Examples
≡≡≡≡≡≡≡≡≡≡
cruises()

"""
function cruises()
    api = API();
    status, response = query(api, "EXEC uspCruises");
    return response
end 



"""
    cruise_by_name(cruiseName::String)

Returns a dataframe containing cruise info using cruise name.
The details include cruise official name, nickname, ship name, start/end time/location, etc …
A full list of cruise names can be retrieved using cruise method.
If applicable, you may also use cruise “nickname” (‘Diel’, ‘Gradients_1’ …). 

Examples
≡≡≡≡≡≡≡≡≡≡
cruise_by_name("KOK1606");
cruise_by_name("Gradients_1");

"""
function cruise_by_name(cruiseName::String)
    api = API();
    status, response = query(api, "EXEC uspCruiseByName '$cruiseName'");
    if nrow(response) < 1
        error("Invalid cruise name: $cruiseName.");
    end    
    if nrow(response) > 1
        println(response);
        error("More than one cruise found (see above). Please provide a more specific name.")
    end    
    return response
end 



"""
    cruise_bounds(cruiseName::String)

Returns a dataframe containing the spatio-temporal bounding box accosiated with the specified cruise.
A full list of cruise names can be retrieved using cruise method.
If applicable, you may also use cruise “nickname” (‘Diel’, ‘Gradients_1’ …).

Examples
≡≡≡≡≡≡≡≡≡≡
cruise_bounds("KOK1606");
cruise_bounds("Gradients_1");

"""
function cruise_bounds(cruiseName::String)
    id = cruise_by_name(cruiseName).ID[1];
    api = API();
    status, response = query(api, "EXEC uspCruiseBounds $id");
    return response
end 



"""
    cruise_trajectory(cruiseName::String)

Returns a dataframe containing the cruise trajectory.
A full list of cruise names can be retrieved using cruise method.
If applicable, you may also use cruise “nickname” (‘Diel’, ‘Gradients_1’ …).

Examples
≡≡≡≡≡≡≡≡≡≡
cruise_trajectory("KOK1606");
cruise_trajectory("Gradients_1");

"""
function cruise_trajectory(cruiseName::String)
    id = cruise_by_name(cruiseName).ID[1];
    api = API();
    status, response = query(api, "EXEC uspCruiseTrajectory $id");
    return response
end 



"""
    cruise_variables(cruiseName::String)

Returns a dataframe containing all registered variables (at Simons CMAP) during a cruise.
A full list of cruise names can be retrieved using cruise method.
If applicable, you may also use cruise “nickname” (‘Diel’, ‘Gradients_1’ …).

Examples
≡≡≡≡≡≡≡≡≡≡
cruise_variables("KOK1606");
cruise_variables("Gradients_1");

"""
function cruise_variables(cruiseName::String)
    id = cruise_by_name(cruiseName).ID[1];
    api = API();
    status, response = query(api, "SELECT * FROM dbo.udfCruiseVariables($id)");
    return response
end 



"""
    _subset(spName::String, 
            table::String, 
            variable::String, 
            dt1::String, 
            dt2::String, 
            lat1::Float64,
            lat2::Float64, 
            lon1::Float64, 
            lon2::Float64, 
            depth1::Float64, 
            depth2::Float64
           )

Returns a subset of data according to space-time constraints.
This method is meant to be used internally.

Examples
≡≡≡≡≡≡≡≡≡≡
_subset("uspSpaceTime", "tblAltimetry_REP", "sla", "2016-01-01", "2016-01-01", 30., 31., -160., -159., 0., 0.);

"""
function _subset(spName::String, 
                table::String, 
                variable::String, 
                dt1::String, 
                dt2::String, 
                lat1::Float64,
                lat2::Float64, 
                lon1::Float64, 
                lon2::Float64, 
                depth1::Float64, 
                depth2::Float64
                )
    statement = "EXEC $spName '$table', '$variable', '$dt1', '$dt2', $lat1, $lat2, $lon1, $lon2, $depth1, $depth2"; 
    api = API();
    status, response = query(api, statement);
    return response
end 



"""
    _interval_to_uspName(interval::String) 

Returns a timeseries-based stored procedure name according to the specified interval.
This method is meant to be used internally.

Examples
≡≡≡≡≡≡≡≡≡≡
_interval_to_uspName("week");

"""
function _interval_to_uspName(interval::String) 
    usp = "";
    if interval == ""
        usp = "uspTimeSeries"
    elseif interval in ["w", "week", "weekly"]   
        usp = "uspWeekly"
    elseif interval in ["m", "month", "monthly"]   
        usp = "uspMonthly"
    elseif interval in ["q", "s", "season", "seasonal", "seasonality", "quarterly"]   
        usp = "uspQuarterly"
    elseif interval in ["a", "y", "year", "yearly", "annual"]   
        usp = "uspAnnual"
    else
        error("Invalid interval: $interval")   
    end       
    return usp
end 



"""
    space_time(;
               table::String, 
               variable::String, 
               dt1::String, 
               dt2::String, 
               lat1::Number,
               lat2::Number, 
               lon1::Number, 
               lon2::Number, 
               depth1::Number, 
               depth2::Number
              )

Returns a subset of data according to the specified space-time constraints (dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2). 
The results are ordered by time, lat, lon, and depth (if exists), respectively.

Examples
≡≡≡≡≡≡≡≡≡≡
space_time(
           table="tblArgoMerge_REP", 
           variable="argo_merge_salinity_adj", 
           dt1="2015-05-01", 
           dt2="2015-05-30", 
           lat1=28.1, 
           lat2=35.4, 
           lon1=-71.3, 
           lon2=-50, 
           depth1=0, 
           depth2=100
           )

"""
function space_time(;
                    table::String, 
                    variable::String, 
                    dt1::String, 
                    dt2::String, 
                    lat1::Number,
                    lat2::Number, 
                    lon1::Number, 
                    lon2::Number, 
                    depth1::Number, 
                    depth2::Number
                    )
    return _subset("uspSpaceTime",
                   table, 
                   variable, 
                   dt1, 
                   dt2, 
                   float(lat1),
                   float(lat2), 
                   float(lon1), 
                   float(lon2), 
                   float(depth1), 
                   float(depth2)
                   )
end 



"""
    time_series(;
               table::String, 
               variable::String, 
               dt1::String, 
               dt2::String, 
               lat1::Number,
               lat2::Number, 
               lon1::Number, 
               lon2::Number, 
               depth1::Number, 
               depth2::Number
              )

Returns a subset of data according to the specified space-time constraints (dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2). 
The returned data subset is aggregated by time: at each time interval, the mean and standard deviation of the variable values within the space-time constraints are computed. 
The sequence of these values construct the timeseries. 
The timeseries data can be binned weekly, monthly, quarterly, or annually, if the interval parameter is set (this feature is not applicable to climatological datasets). 
The resulted timeseries is returned in the form of a dataframe ordered by time.

Examples
≡≡≡≡≡≡≡≡≡≡
time_series(
           table="tblAltimetry_REP", 
           variable="adt", 
           dt1="2016-01-01", 
           dt2="2018-01-01", 
           lat1=33, 
           lat2=35, 
           lon1=-160, 
           lon2=-159, 
           depth1=0, 
           depth2=0,
           interval="monthly"
           )

"""
function time_series(;
                    table::String, 
                    variable::String, 
                    dt1::String, 
                    dt2::String, 
                    lat1::Number,
                    lat2::Number, 
                    lon1::Number, 
                    lon2::Number, 
                    depth1::Number, 
                    depth2::Number,
                    interval::String=""
                    )
    usp = _interval_to_uspName(interval);
    if (usp != "uspTimeSeries") && (is_climatology(table))
        error(
              """
              Custom binning (monthly, weekly, ...) is not suppoerted for climatological data sets. 
              Table $table represents a climatological data set.
              """
            );
    end    
    return _subset(usp,
                   table, 
                   variable, 
                   dt1, 
                   dt2, 
                   float(lat1),
                   float(lat2), 
                   float(lon1), 
                   float(lon2), 
                   float(depth1), 
                   float(depth2)                   
                   )
end 



"""
    depth_profile(;
                 table::String, 
                 variable::String, 
                 dt1::String, 
                 dt2::String, 
                 lat1::Number,
                 lat2::Number, 
                 lon1::Number, 
                 lon2::Number, 
                 depth1::Number, 
                 depth2::Number
                )

Returns a subset of data according to the specified space-time constraints (dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2). 
The returned data subset is aggregated by depth: at each depth level the mean and standard deviation of the variable values within the space-time constraints are computed. 
The sequence of these values construct the depth profile. 
The resulting depth profile is returned in the form of a Pandas dataframe ordered by depth.
          
Examples
≡≡≡≡≡≡≡≡≡≡
depth_profile(
             table="tblPisces_NRT", 
             variable="CHL", 
             dt1="2016-04-30", 
             dt2="2016-04-30", 
             lat1=20, 
             lat2=24, 
             lon1=-170, 
             lon2=-150, 
             depth1=0, 
             depth2=500
             )

"""
function depth_profile(;
                      table::String, 
                      variable::String, 
                      dt1::String, 
                      dt2::String, 
                      lat1::Number,
                      lat2::Number, 
                      lon1::Number, 
                      lon2::Number, 
                      depth1::Number, 
                      depth2::Number
                      )
    return _subset("uspDepthProfile",
                   table, 
                   variable, 
                   dt1, 
                   dt2, 
                   float(lat1),
                   float(lat2), 
                   float(lon1), 
                   float(lon2), 
                   float(depth1), 
                   float(depth2)
                   )
end 



"""
    section(;
            table::String, 
            variable::String, 
            dt1::String, 
            dt2::String, 
            lat1::Number,
            lat2::Number, 
            lon1::Number, 
            lon2::Number, 
            depth1::Number, 
            depth2::Number
           )

Returns a subset of data according to the specified space-time constraints.
The results are ordered by time, lat, lon, and depth.

Examples
≡≡≡≡≡≡≡≡≡≡
section(
        table="tblPisces_NRT", 
        variable="NO3", 
        dt1="2016-04-30", 
        dt2="2016-04-30", 
        lat1=10, 
        lat2=50, 
        lon1=-159, 
        lon2=-158, 
        depth1=0, 
        depth2=500
        )

"""
function section(;
                table::String, 
                variable::String, 
                dt1::String, 
                dt2::String, 
                lat1::Number,
                lat2::Number, 
                lon1::Number, 
                lon2::Number, 
                depth1::Number, 
                depth2::Number
                )
    return _subset("uspSectionMap",
                   table, 
                   variable, 
                   dt1, 
                   dt2, 
                   float(lat1),
                   float(lat2), 
                   float(lon1), 
                   float(lon2), 
                   float(depth1), 
                   float(depth2)
                   )
end 

