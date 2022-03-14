# Update

So today I've succesfully generated a Go client for the nats-token-exchange api. The next step is to write the code that actually interfaces with it so that engines can be configured to authenticate automatically. The goal here is that you do so onme of the following:

* Provide a client_id and client_secrate and the engine will:
    * Auth with Auth0 and get a token
    * Generate NKeys if required (or maybe always generate them? More secure?)
    * Use that token to get a NATS token
    * Authenticate to NATS
    * Re-generate NATS and OAuth tokens as required
* Provide an OAuth token and do all of the above except authenticating with auth0 and getting a new oAuth token when it expires
    * This model is designed for use when the tokens are managed by an enternal party like srcman

One difficult question is going to be how do we test this...

**UPDATE:** I have now added a token exchange container, however I need to fix this issue before it will start: https://github.com/overmindtech/nats-token-exchange/issues/17