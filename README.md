# Keboola-Intercom-Event-Writer

This uploads Liftago specific events from Keboola into Intercom. 

## Keboola Config JSON:
- `personalAccessToken`: Intercom's Personal Access Token, that identifies the app
- `timeoutBetweenAPICalls`: Timeout between API Calls. Intercom's API cannot proceed unlimited amount of requests. Timeouts prevent crash of the app.
- `maxItemsPerRequest`: How many items can be bulk-uploaded at once. 

## Prefered Settings:
``` `maxItemsPerRequest` = 100, `timeoutBetweenAPICalls` = 3 (or more) ```
