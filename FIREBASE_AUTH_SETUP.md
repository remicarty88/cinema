# Firebase Auth rollout

Do not deploy the Service Account JSON that was previously shared. Its private
key must be revoked in Google Cloud IAM, then replaced with a newly generated
key. Never put the replacement JSON in this repository or send it in chat.

## Firebase Console

1. In Firebase Console, open **Authentication → Sign-in method** and enable
   **Anonymous**. The site uses anonymous Auth for guests and Firebase custom
   tokens for username/password accounts.
2. Copy the Web API key from **Project settings → General → Web app**. This key
   is public client configuration, not a private credential.
3. In Google Cloud Console, revoke the exposed Service Account key and create a
   replacement JSON key for the Firebase Admin SDK.

## Railway Variables

Keep the existing database URL and root, and set:

```text
FIREBASE_WEB_API_KEY=<Firebase Web API key>
FIREBASE_SERVICE_ACCOUNT_JSON=<entire replacement Service Account JSON>
FIREBASE_DB_URL=https://neonapp-a05b0-default-rtdb.firebaseio.com
FIREBASE_ROOT=kino88
ADMIN_TOKEN=<new long random secret>
```

`FIREBASE_SERVICE_ACCOUNT_JSON` is secret and must exist only in Railway
Variables. The Web API key is returned by `/api/config` for the browser SDK.

## Rules Simulator Checks

Before publishing `firebase.database.rules.json`, verify these cases in the
Realtime Database Rules Playground/Simulator:

1. Unauthenticated reads and writes anywhere under `kino88` are denied.
2. An authenticated user can read and write only `users/<their-uid>`; another
   UID and every `accounts/<login>` path are denied to a non-admin.
3. An authenticated user can increment `views/<film>` by one, but cannot
   decrement it or replace it with a non-number.
4. A room host can create its 16-character room and change `meta/ctl`. A user
   with the room code can join and use room paths; a non-member cannot read
   messages, voice, playback, or signaling data.
5. A member can create a chat message only with their own UID. An admin token
   with the `admin: true` claim can read/write the dashboard data and post an
   admin chat message.
6. Authenticated clients can access only 24-character remote-control codes.

## Deploy Order

1. Deploy `server.py`, `index.html`, `analytics.py`, `admin.html`,
   `firebase_admin_support.py`, and the updated `requirements.txt`.
2. Confirm login, registration, rooms, chat, remote control, and `/admin` work.
   Existing registered accounts keep their UID and data; users may need to log
   in once again. Guest-only history is not securely migrated. Existing room
   codes and remote-control codes are intentionally replaced.
3. Only after the new client and server are deployed, publish the rules from
   `firebase.database.rules.json` in **Realtime Database → Rules**.

Publishing the rules before deploying the new code blocks Firebase features.
The Admin SDK on Railway bypasses client Rules for account verification and
admin operations; browser clients use authenticated Firebase UIDs and granular
Rules.
