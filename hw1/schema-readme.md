1. Advertiser 
Avoid repeating advertiser names across campaigns.

2. Campaign
Define campaigns uniquely and link them to advertisers.
Instead of repeating advertiser names, we link to the Advertiser table.

 3. User
Centralized user data with demographics and preferences.

4. AdEvent
Stores each ad impression and optionally a click event.
Links users and campaigns directly. WasClicked and ClickTimestamp allow for join-free click analysis.

5. Interest + UserInterest
To normalize user interests further.

✅ Why this structure?
Data integrity: Avoids repeating advertiser names and interest lists.
