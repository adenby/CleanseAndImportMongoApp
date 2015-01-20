cls
REM -Xmx3000m -Xms3000m 
REM java -jar target/CleanseAndImportMongoApp-1.0-SNAPSHOT.jar SwanOnly SwanDataDirectory="C:/Workspaces/jobsite/trufa/Common-Data/Swan"

REM java -jar target/CleanseAndImportMongoApp-1.0-SNAPSHOT.jar SwanDataDirectory="C:/Workspaces/jobsite/trufa/Common-Data/Swan" ScrapingHubDataDirectory="C:/Workspaces/jobsite/trufa/Common-Data/ScrapingHub"

java -jar target/CleanseAndImportMongoApp-1.0-SNAPSHOT.jar ScrapingHubOnly ScrapingHubDataDirectory="C:/Workspaces/jobsite/trufa/Common-Data/ScrapingHubSmall"
