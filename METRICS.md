# Metric Table

#### User Demographics + Usage Metrics

| Metric Name | Description |
|-------------|-------------|
|OS Distribution| % of clients with a given add-on by OS. Is an add-on more frequently used on specific OS’s?|
|Country Distribution | % of clients with a given add-on by country. Are certain countries more likely to install a given add-on?|
|Active Hours | Mean active hours by client, compared the the general population as a baseline. Are clients with a given add-on more or less active than the rest of the population?|
|Total URIs | Mean Total URI count by client (number of total page loads), comparing to the general population as a baseline. Do clients with a given add-on browse the web more or less than the general population?|
|Tabs Opened | Distribution of number of tabs opened (or an aggregate statistic), compared to the general population. Are users with a given add-on opening more/less tabs?|
|Bookmarks Opened | Number of bookmarks per client compared to the general population. Are users with a given add-on saving more pages?|
|Other add-ons  | The top add-ons often seen in conjunction with a given add-on. Users with add-on x are likely to also install add-on y, z, …, etc.|
|Devtools Panels Count | Number of times a given devtools panel, i.e. the inspector, or devtools in some form was used, compared to the general population. Are user’s with a given add-on using specific devtools features more often than usual?|
|Tracking Protection Enabled | % of users with a given add-on that have enabled tracking protection. Are uses with a specific add-on more concerned about privacy/tracking?|

#### Install Flow + Meta Metrics


| Metric Name | Description |
|-------------|-------------|
|Number of installs (by source) | Daily installs per add-on from AMO and about:addons|
|Number of impressions | Daily visits to add-on specific AMO property for the top 10 locales|
|Average Rating | Average daily star rating of a given add-on|
|Total Reviews | Number of reviews for a given add-on, cumulative by date|


#### Performance Metrics


| Metric Name | Description |
|-------------|-------------|
|Background Page Load Time | The amount of time it takes for a WebExtension’s background page to load, compared to some baseline.|
|Browser Action Popup Load Time | The amount of time it takes for a WebExtension’s browser action to open, compared to some baseline.|
|Page Action Popup Load Time | The amount of time it takes for a WebExtension’s page action to open, compared to some baseline.|
|Content Script Injection Time | The amount of time it takes for content scripts from a WebExtension to be injected into a window, compared to some baseline.|
|Storage Local ‘get’ Time | The amount of time it takes to perform a “get” operation via local storage.|
|Storage Local ‘set’ Time | The amount of time it takes to perform a “set” operation via local storage.|
|Startup Time | The time it takes for a WebExtension to start up, compared to some baseline|
|Crashes | Mean number of crashes per hour for clients with a given add-on. Are users with a given add-on more likely to experience a crash?|
|Page Load Times | (pre-release only): Distribution of Page load times (or an aggregate statistic), compared to the general population. Are users with a given add-on having a slower/faster browsing experience?|
|Tab Switch Time | Distribution of tab switch times (or an aggregate statistic), compared to the general population. Are users with a given add-on having a slower/faster tab switching experience?|


#### Trend Metrics


| Metric Name | Description |
|-------------|-------------|
|MAU | Monthly Active Users, the number of unique clients with a given add-on over a 28 day period. We already plan to expose this in the Public Data Report.|
|YAU | Yearly Active Users, the number of unique clients with a given add-on over a 28 day period. We already plan to expose this in the Public Data Report.|
