# rqman
rqman is a simple web UI for [rqlite](https://github.com/rqlite/rqlite). You can execute SQL queries and check status of rqlite cluster via rqman.  

## Demo
[https://rqlite.soonoo.me](https://rqlite.soonoo.me)

## Installation
### 1. With Docker
rqman should be up and running on port 80 with following `docker run` command.
```
docker run -p 80:3000 \
  -e RQMAN_USERNAME=user \
  -e RQMAN_PASSWORD=pass \
  -e RQLITE_HOST=http://localhost:4001 \
  soonoo/rqman:0.0.3
```

Please refer to the table below for available environment variables.
| Key            | Required | Example value         | Default value         | Notes                                                                           |
|----------------|----------|-----------------------|-----------------------|---------------------------------------------------------------------------------|
| RQMAN_USERNAME | No       | user                  |                       | Both RQMAN_USERNAME and RQMAN_PASSWORD must be provided to use authentication.  |
| RQMAN_PASSWORD | No       | pass1234              |                       |                                                                                 |
| RQLITE_HOST    | No       | http://localhost:4001 | http://localhost:4001 | Accessible rqlite endpoint                                                      |

### 2. Building from source
With Node.js 16 or higher and yarn installed, you can build rqlite from source
```
git clone https://github.com/soonoo/rqman
cd rqman
yarn install && yarn build
yarn start
```