import sys

if len(sys.argv) != 3:
    print("Usage: {} <output-file> <clients-number>".format(sys.argv[0]))
    sys.exit(1)

output_file = sys.argv[1]

try:
    clients_num = int(sys.argv[2])
except ValueError:
    print("The amount of clients must be an integer.")
    sys.exit(1)

compose_content = "name: tp0\n"
compose_content += "services:\n"

compose_content += "  server:\n"
compose_content += "    container_name: server\n"
compose_content += "    image: server:latest\n"
compose_content += "    entrypoint: python3 /main.py\n"
compose_content += "    environment:\n"
compose_content += "      - PYTHONUNBUFFERED=1\n"
# compose_content += "      - LOGGING_LEVEL=DEBUG\n"
compose_content += "    networks:\n"
compose_content += "      - testing_net\n\n"
compose_content += "    volumes:\n"
compose_content += "      - ./server/config.ini:/server/config.ini\n\n"


# Client env variables
name = "Santiago Lionel"
last_name = "Lorca"
dni = "30904465"
birth_date = "1999-03-17"
number = "7574"

for i in range(1, clients_num + 1):
    compose_content += f"  client{i}:\n"
    compose_content += f"    container_name: client{i}\n"
    compose_content += "    image: client:latest\n"
    compose_content += "    entrypoint: /client\n"
    compose_content += "    environment:\n"
    compose_content += f"      - CLI_ID={i}\n"
    # compose_content += "      - CLI_LOG_LEVEL=DEBUG\n"
    compose_content += f"      - NOMBRE={name}\n"
    compose_content += f"      - APELLIDO={last_name}\n"
    compose_content += f"      - DOCUMENTO={dni}\n"
    compose_content += f"      - NACIMIENTO={birth_date}\n"
    compose_content += f"      - NUMERO={number}\n"
    compose_content += "    networks:\n"
    compose_content += "      - testing_net\n"
    compose_content += "    depends_on:\n"
    compose_content += "      - server\n\n"
    compose_content += "    volumes:\n"
    compose_content += "      - ./client/config.yaml:/config.yaml \n"
    compose_content += f"      - ./.data/agency-{i}.csv:/agency.csv \n\n"


compose_content += "networks:\n"
compose_content += "  testing_net:\n"
compose_content += "    ipam:\n"
compose_content += "      driver: default\n"
compose_content += "      config:\n"
compose_content += "        - subnet: 172.25.125.0/24\n"

with open(output_file, "w") as file:
    file.write(compose_content)

print(f"File {output_file} generated successfully with a server and {clients_num} clients.")
    