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
compose_content += "      - LOGGING_LEVEL=DEBUG\n"
compose_content += "    networks:\n"
compose_content += "      - testing_net\n\n"
compose_content += "    volumes:\n"
compose_content += "      - ./server/config.ini:/config/config.ini\n\n"

for i in range(1, clients_num + 1):
    compose_content += f"  client{i}:\n"
    compose_content += f"    container_name: client{i}\n"
    compose_content += "    image: client:latest\n"
    compose_content += "    entrypoint: /client\n"
    compose_content += "    environment:\n"
    compose_content += f"      - CLI_ID={i}\n"
    compose_content += "      - CLI_LOG_LEVEL=DEBUG\n"
    compose_content += "    networks:\n"
    compose_content += "      - testing_net\n"
    compose_content += "    depends_on:\n"
    compose_content += "      - server\n\n"
    compose_content += "    volumes:\n"
    compose_content += "      - ./client/config.yaml:/config.yaml \n\n"

compose_content += "networks:\n"
compose_content += "  testing_net:\n"
compose_content += "    ipam:\n"
compose_content += "      driver: default\n"
compose_content += "      config:\n"
compose_content += "        - subnet: 172.25.125.0/24\n"

with open(output_file, "w") as file:
    file.write(compose_content)

print(f"File {output_file} generated successfully with a server and {clients_num} clients.")
    