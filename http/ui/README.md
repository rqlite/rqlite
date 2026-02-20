# rqlite UI
Rqlite UI provides simple web UI for rqlite. You can execute SQL queries and check status of rqlite cluster via rqman.  


## Development
### Prerequisite
Latest [Node.js](https://nodejs.org/ko/download) and [Yarn](https://classic.yarnpkg.com/lang/en/docs/install/#mac-stable) are needed.

### Running UI in local environment
After executing below command, UI will be running on `localhost:3000`
```bash
yarn dev
```


## Packaging into rqlited
```bash
yarn build && yarn next export
```
The build output can be found in the `out` directory. When you build rqlited, that directory will be included in the binary.  
