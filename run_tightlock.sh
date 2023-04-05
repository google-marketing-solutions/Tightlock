ENV=".env"
if ! [ -f $ENV ]; then
  # create env file and write Airflow UID ang GID to it
  echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > $ENV

  # generate or read API key
  PSEUDORANDOM_API_KEY=$( cat /dev/urandom | tr -dc '[:alpha:]' | fold -w20 | head -n 1 )
  if ! [ $1 == "--non-interactive" ]; then
    echo "Choose API key generation method."
    select yn in "User-provided" "Pseudorandom"; do
        case $yn in
            User-provided ) read -p "Enter API KEY: " API_KEY; break;;
            Pseudorandom ) API_KEY=$PSEUDORANDOM_API_KEY; echo "API key: ${API_KEY}"; break;;
        esac
    done
  else
    API_KEY=$PSEUDORANDOM_API_KEY
  fi
  # append API key to env file
  echo -e "TIGHTLOCK_API_KEY=$API_KEY" >> $ENV
fi

docker-compose up --build -d
