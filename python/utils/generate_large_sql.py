import random
from datetime import datetime, timedelta

last_location_id = 0
last_department_id = 0
last_job_id = 0
last_employee_id = 0
last_dependent_id = 0

current_employees = {}
current_dependents = []

def generate_begin_instructions() -> list[str]:
    instructions = [
        "## metadata",
        "### autocommit false\n",
        "## test"
    ]
    return instructions

def generate_tables_create() -> list[str]:
    table_creation_statements = [
        f"BEGIN;",

        # create regions table
        f"\nCREATE TABLE IF NOT EXISTS regions (",
        f"    region_id SERIAL PRIMARY KEY,",
        f"    region_name CHARACTER VARYING (25)",
        f");",

        # create countries table
        f"\nCREATE TABLE IF NOT EXISTS countries (",
        f"    country_id CHARACTER (2) PRIMARY KEY,",
        f"    country_name CHARACTER VARYING (40),",
        f"    region_id INTEGER NOT NULL,",
        f"    FOREIGN KEY (region_id) REFERENCES regions (region_id) ON UPDATE CASCADE ON DELETE CASCADE",
        f");",

        # create locations table
        f"\nCREATE TABLE IF NOT EXISTS locations (",
        f"    location_id SERIAL PRIMARY KEY,",
        f"    street_address CHARACTER VARYING (40),",
        f"    postal_code CHARACTER VARYING (12),",
        f"    city CHARACTER VARYING (30) NOT NULL,",
        f"    state_province CHARACTER VARYING (35),",
        f"    country_id CHARACTER (2) NOT NULL,",
        f"    FOREIGN KEY (country_id) REFERENCES countries (country_id) ON UPDATE CASCADE ON DELETE CASCADE",
        f");",

        # create departments table
        f"\nCREATE TABLE IF NOT EXISTS departments (",
        f"    department_id SERIAL PRIMARY KEY,",
        f"    department_name CHARACTER VARYING (30) NOT NULL,",
        f"    location_id INTEGER,",
        f"    FOREIGN KEY (location_id) REFERENCES locations (location_id) ON UPDATE CASCADE ON DELETE CASCADE",
        f");",

        # create jobs table
        f"\nCREATE TABLE IF NOT EXISTS jobs (",
        f"    job_id SERIAL PRIMARY KEY,",
        f"    job_title CHARACTER VARYING (35) NOT NULL,",
        f"    min_salary NUMERIC (8, 2),",
        f"    max_salary NUMERIC (8, 2)",
        f");",

        # create employees table
        f"\nCREATE TABLE IF NOT EXISTS employees (",
        f"    employee_id SERIAL PRIMARY KEY,",
        f"    first_name CHARACTER VARYING (20),",
        f"    last_name CHARACTER VARYING (25) NOT NULL,",
        f"    email CHARACTER VARYING (100) NOT NULL,",
        f"    phone_number CHARACTER VARYING (20),",
        f"    hire_date DATE NOT NULL,",
        f"    job_id INTEGER NOT NULL,",
        f"    salary NUMERIC (15, 2) NOT NULL,",
        f"    manager_id INTEGER,",
        f"    department_id INTEGER,",
        f"    FOREIGN KEY (job_id) REFERENCES jobs (job_id) ON UPDATE CASCADE ON DELETE CASCADE,",
        f"    FOREIGN KEY (department_id) REFERENCES departments (department_id) ON UPDATE CASCADE ON DELETE CASCADE,",
        f"    FOREIGN KEY (manager_id) REFERENCES employees (employee_id) ON UPDATE CASCADE ON DELETE CASCADE",
        f");",

        # create dependents table
        f"\nCREATE TABLE IF NOT EXISTS dependents (",
        f"    dependent_id SERIAL PRIMARY KEY,",
        f"    first_name CHARACTER VARYING (50) NOT NULL,",
        f"    last_name CHARACTER VARYING (50) NOT NULL,",
        f"    relationship CHARACTER VARYING (25) NOT NULL,",
        f"    employee_id INTEGER NOT NULL,",
        f"    FOREIGN KEY (employee_id) REFERENCES employees (employee_id) ON DELETE CASCADE ON UPDATE CASCADE",
        f");",

        f"\nCOMMIT;"
    ]
    return table_creation_statements

def generate_regions_insert_instructions(regions_list: list[list[int, str]]) -> list[str]:
    insert_statements = []
    insert_statements.append(f"\n-- INSERTIN REGIONS --")
    insert_statements.append(f"BEGIN;")
    for region in regions_list:
        region_id = region[0]
        region_name = region[1]
        insert_statements.append(f"INSERT INTO regions (region_id, region_name) VALUES ({region_id}, '{region_name}');")
    insert_statements.append(f"COMMIT;")
    return insert_statements

def generate_countries_insert_instructions(regions_list: list[list[int, str]], countries_list: dict[list[str, str]]) -> list[str]:
    insert_statements = []
    insert_statements.append(f"\n-- INSERTING COUNTRIES --")
    insert_statements.append(f"BEGIN;")
    for region in regions_list:
        region_id = region[0]
        region_name = region[1]
        insert_statements.append(f"\n-- INSERTING COUNTRIES of {region_name} --")
        countries = countries_list[region_id]
        for country in countries:
            country_code = country[0]
            country_name = country[1]
            insert_statements.append(f"INSERT INTO countries (country_id, country_name, region_id) VALUES ('{country_code}', '{country_name}', {region_id});")
    insert_statements.append(f"COMMIT;")
    return insert_statements


def generate_locations_insert_instructions(regions_list: list[list[int, str]], countries_list: dict[list[str, str]]) -> list[str]:
    global last_location_id
    insert_statements = []
    insert_statements.append(f"\n-- INSERTING LOCATIONS --")
    insert_statements.append(f"BEGIN;")
    for region in regions_list:
        region_id = region[0]
        region_name = region[1]
        insert_statements.append(f"\n-- INSERTING LOCATIONS of {region_name} --")
        countries = countries_list[region_id]
        for country in countries:
            last_location_id += 1
            country_code = country[0]
            country_name = country[1]
            street_address = country[2]
            postal_code = country[3]
            city = country[4]
            province = country[5]
            insert_statements.append(f"-- INSERTING LOCATION for {country_name} --")
            insert_statements.append(f"INSERT INTO locations (location_id, street_address, postal_code, city, state_province, country_id) VALUES ({last_location_id}, '{street_address}', '{postal_code}', '{city}', '{province}', '{country_code}');")
    insert_statements.append(f"COMMIT;")
    return insert_statements

def generate_departments_insert_instructions(department_list: list[str]) -> list[str]:
    global last_location_id
    global last_department_id
    start_location = 1
    end_location = last_location_id
    department = 0
    department_length = len(department_list)
    insert_statements = []
    insert_statements.append(f"\n-- INSERTING DEPARTMENTS --")
    insert_statements.append(f"BEGIN;")
    for location in range(start_location, end_location + 1):
        iter = 0
        while iter < 3:
            last_department_id += 1
            insert_statement = f"INSERT INTO departments (department_id, department_name, location_id) VALUES ({last_department_id}, '{department_list[department]}', {location});"
            department = (department + 1) % department_length
            iter += 1
            insert_statements.append(insert_statement)
    insert_statements.append(f"COMMIT;")
    return insert_statements

def generate_jobs_insert_instructions(job_list: list[str, float, float]) -> list[str]:
    global last_job_id
    insert_statements = []
    insert_statements.append(f"\n-- INSERTING JOBS --")
    insert_statements.append(f"BEGIN;")
    for job in job_list:
        last_job_id += 1
        job_title = job[0]
        min_salary = job[1]
        max_salary = job[2]
        insert_statements.append(f"INSERT INTO jobs (job_id, job_title, min_salary, max_salary) VALUES ({last_job_id}, '{job_title}', {min_salary:.2f}, {max_salary:.2f});")
    insert_statements.append(f"COMMIT;")
    return insert_statements

def generate_email(first_name: str, last_name: str, domain="gmail.com") -> str:
    return f"{first_name.lower()}.{last_name.lower()}@{domain}"

def generate_random_date(start: str, end: str) -> str:
    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y-%m-%d")
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    random_date = start_date + timedelta(days=random_days)
    return random_date.strftime("%Y-%m-%d")

def generate_insert_dependent_statement(dependent_name: str, employee_id: int, dependent_ids_list: list[int]) -> str:
    global last_dependent_id
    global current_dependents

    dependent_first_name, dependent_last_name = dependent_name.split(" ")
    relationship = random.choice(["child", "spouse", "parent"])
    last_dependent_id += 1
    current_dependents.append(last_dependent_id)
    dependent_ids_list.append(last_dependent_id)
    insert_dependent_statement = \
        f"INSERT INTO dependents (dependent_id, first_name, last_name, relationship, employee_id) " \
        f"VALUES ({last_dependent_id}, '{dependent_first_name}', '{dependent_last_name}', '{relationship}', {employee_id});"
    return insert_dependent_statement

def generate_insert_employee_statement(employe_list: list, dependent_list: list) -> list[str]:
    global last_department_id
    global last_job_id
    global last_employee_id
    current_data = random.choice(employe_list)

    first_name, last_name = current_data[0].split(" ")
    phone_number = current_data[1]
    hire_date = generate_random_date("1990-01-01", "2025-12-31")
    salary = current_data[2]
    job_id = random.randint(1, last_job_id)
    department_id = random.randint(1, last_department_id)

    last_employee_id += 1

    if last_employee_id != 1:
        # get all employees ids
        all_employees_ids = sorted(current_employees.keys())
        # choose one randomly to as the manager
        manager_employee_id = random.choice(all_employees_ids)
    else:
        # if this is the first employee, set the manager to NULL
        manager_employee_id = "NULL"

    insert_statements = []
    insert_statements.append(f"\n-- INSERTING EMPLOYEE {last_employee_id} --")
    insert_statements.append(f"BEGIN;")
    insert_statement = \
        f"INSERT INTO employees (employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary,manager_id,department_id)" \
        f" VALUES ({last_employee_id}, '{first_name}', '{last_name}', '{generate_email(first_name, last_name)}', '{phone_number}', '{hire_date}', {job_id}, {salary:.2f}, {manager_employee_id}, {department_id});"

    insert_statements.append(insert_statement)

    # Random number of dependents (1-5)
    num_dependents = random.randint(0, 4)
    random_dependents = random.sample(dependent_list, num_dependents)
    dependent_ids_list = []
    for dependent in random_dependents:
        insert_statements.append(generate_insert_dependent_statement(dependent, last_employee_id, dependent_ids_list))

    current_employees[last_employee_id] = {
        "insert": insert_statement,
        "direct_reports": [],
        "dependents": dependent_ids_list
    }
    if last_employee_id != 1:
        current_employees[manager_employee_id]["direct_reports"].append(last_employee_id)
    insert_statements.append(f"COMMIT;")
    return insert_statements

def generate_update_manager_id_statement(employee_id: int, manager_id: int) -> str:
    update_statement = f"UPDATE employees SET manager_id = {manager_id} WHERE employee_id = {employee_id};"
    global current_employees
    current_employees[manager_id]["direct_reports"].append(employee_id)
    return update_statement

def generate_delete_employee_statement() -> list[str]:
    global current_employees
    global current_dependents
    # get all employees ids except for CEO
    all_employees_ids = sorted(current_employees.keys())[1:]

    if len(all_employees_ids) == 1:
        return []

    # choose one randomly to delete
    delete_employee_id = random.choice(all_employees_ids)

    # get all direct reports
    direct_reports = current_employees[delete_employee_id]["direct_reports"]
    # find a new manager for the direct reports
    if len(direct_reports) != 0:
        all_employees_ids.remove(delete_employee_id)
        new_manager_id = random.choice(all_employees_ids)

    dependents = current_employees[delete_employee_id]["dependents"]
    current_dependents = list(set(current_dependents) - set(dependents))

    delete_statements = []
    delete_statements.append(f"\n-- DELETING EMPLOYEE {delete_employee_id} --")
    delete_statements.append(f"BEGIN;")

    # generate manager id update statements
    for report_id in direct_reports:
        delete_statements.append(generate_update_manager_id_statement(report_id, new_manager_id))

    delete_statements.append(f"DELETE FROM dependents WHERE employee_id = {delete_employee_id};")
    delete_statements.append(f"DELETE FROM employees WHERE employee_id = {delete_employee_id};")
    del current_employees[delete_employee_id]

    delete_statements.append(f"COMMIT;")
    return delete_statements

def generate_delete_dependent_statement() -> list[str]:
    global current_dependents
    random_dependent = random.choice(current_dependents)
    current_dependents.remove(random_dependent)

    # NOTE: we are not updating the list of dependents per employee
    delete_statements = []
    delete_statements.append(f"\n-- DELETING DEPENDENT {random_dependent} --")
    delete_statements.append(f"BEGIN;")

    delete_statements.append(f"DELETE FROM dependents WHERE dependent_id = {random_dependent};")

    delete_statements.append(f"COMMIT;")
    return delete_statements


def generate_insert_dependent_statements(dependent_list: list) -> list[str]:
    global current_employees
    random_dependent = random.choice(dependent_list)
    employee_id = random.choice(list(current_employees.keys()))

    insert_statements = []
    insert_statements.append(f"\n-- INSERTING DEPENDENT {employee_id} --")
    insert_statements.append(f"BEGIN;")

    insert_statements.append(generate_insert_dependent_statement(random_dependent, employee_id, current_employees[employee_id]["dependents"]))
    insert_statements.append(f"END;")

    return insert_statements

def generate_update_salary_statement(max_increase: int) -> list[str]:
    global current_employees
    all_employees_ids = sorted(current_employees.keys())
    employee_id = random.choice(all_employees_ids)
    salary_increase = random.randint(0, max_increase)
    update_statements = []
    update_statements.append(f"\n-- UPDATING EMPLOYEE SALARY {employee_id} --")
    update_statements.append(f"BEGIN;")
    update_statements.append(f"UPDATE employees SET salary = salary + {salary_increase} WHERE employee_id = {employee_id};")
    update_statements.append(f"COMMIT;")
    return update_statements

def generate_end_instructions() -> list[str]:
    instructions = [
        f"\n-- VERIFY --",
        f"## verify",
        f"SELECT * FROM regions ORDER BY region_id;",
        f"SELECT * FROM countries ORDER BY country_id;",
        f"SELECT * FROM locations ORDER BY location_id;",
        f"SELECT * FROM jobs ORDER BY job_id;",
        f"SELECT * FROM departments ORDER BY department_id;",
        f"SELECT * FROM employees ORDER BY employee_id;",
        f"SELECT * FROM dependents ORDER BY dependent_id;",

        f"\n-- END OF TEST --",
        f"## cleanup",
        f"BEGIN;",
        f"DROP TABLE IF EXISTS dependents CASCADE;",
        f"DROP TABLE IF EXISTS employees CASCADE;",
        f"DROP TABLE IF EXISTS departments CASCADE;",
        f"DROP TABLE IF EXISTS jobs CASCADE;",
        f"DROP TABLE IF EXISTS locations CASCADE;",
        f"DROP TABLE IF EXISTS countries CASCADE;",
        f"DROP TABLE IF EXISTS regions CASCADE;",
        f"COMMIT;"
    ]
    return instructions

# Example usage
if __name__ == "__main__":
    regions_list = [
        [1, "Europe"],
        [2, "North America"],
        [3, "South America"],
        [4, "Central America"],
        [5, "Asia"],
        [6, "Africa"],
        [7, "Oceania"],
        [8, "The Caribbean"]
    ]

    region_countries_list = {
        # Countries of Europe
        1: [
            ["AL", "Albania", "Rruga e Dibrës 45", "1001", "Tirana", "Tirana County"],
            ["AD", "Andorra", "Avinguda Meritxell 15", "AD500", "Andorra la Vella", "Andorra la Vella"],
            ["AM", "Armenia", "Abovyan St 23", "0001", "Yerevan", "Yerevan"],
            ["AT", "Austria", "Mariahilfer Straße 88", "1070", "Vienna", "Vienna"],
            ["BY", "Belarus", "Nezavisimosti Ave 58", "220005", "Minsk", "Minsk Region"],
            ["BE", "Belgium", "Rue de la Loi 175", "1048", "Brussels", "Brussels-Capital"],
            ["BA", "Bosnia and Herzegovina", "Zmaja od Bosne 33", "71000", "Sarajevo", "Sarajevo Canton"],
            ["BG", "Bulgaria", "Tsarigradsko shose 47", "1784", "Sofia", "Sofia City Province"],
            ["HR", "Croatia", "Ilica 10", "10000", "Zagreb", "City of Zagreb"],
            ["CY", "Cyprus", "Makariou Avenue 19", "1065", "Nicosia", "Nicosia District"],
            ["CZ", "Czech Republic", "Václavské náměstí 39", "11000", "Prague", "Prague Region"],
            ["DK", "Denmark", "Østerbrogade 45", "2100", "Copenhagen", "Capital Region"],
            ["EE", "Estonia", "Pärnu mnt 18", "10141", "Tallinn", "Harju County"],
            ["FI", "Finland", "Mannerheimintie 20", "00100", "Helsinki", "Uusimaa"],
            ["FR", "France", "Champs-Élysées 102", "75008", "Paris", "Île-de-France"],
            ["GE", "Georgia", "Rustaveli Ave 31", "0108", "Tbilisi", "Tbilisi"],
            ["DE", "Germany", "Kurfürstendamm 26", "10719", "Berlin", "Berlin"],
            ["GR", "Greece", "Ermou Street 15", "10563", "Athens", "Attica"],
            ["HU", "Hungary", "Andrássy út 52", "1062", "Budapest", "Central Hungary"],
            ["IS", "Iceland", "Laugavegur 55", "101", "Reykjavík", "Capital Region"],
            ["IE", "Ireland", "Grafton Street 12", "D02", "Dublin", "Leinster"],
            ["IT", "Italy", "Via del Corso 85", "00186", "Rome", "Lazio"],
            ["XK", "Kosovo", "Nëna Terezë Street 3", "10000", "Pristina", "Pristina District"],
            ["LV", "Latvia", "Brīvības iela 23", "LV-1050", "Riga", "Riga Region"],
            ["LI", "Liechtenstein", "Städtle 12", "9490", "Vaduz", "Oberland"],
            ["LT", "Lithuania", "Gedimino pr. 9", "01103", "Vilnius", "Vilnius County"],
            ["LU", "Luxembourg", "Boulevard Royal 14", "2449", "Luxembourg", "Luxembourg District"],
            ["MT", "Malta", "Republic Street 45", "VLT1115", "Valletta", "Southern Harbour"],
            ["MD", "Moldova", "Ștefan cel Mare Blvd 75", "2001", "Chișinău", "Chișinău Municipality"],
            ["MC", "Monaco", "Boulevard des Moulins 18", "98000", "Monaco", "Monte Carlo"],
            ["ME", "Montenegro", "Njegoševa 22", "81000", "Podgorica", "Podgorica Municipality"],
            ["NL", "Netherlands", "Damrak 70", "1012 LM", "Amsterdam", "North Holland"],
            ["MK", "North Macedonia", "Makedonija Street 5", "1000", "Skopje", "Skopje Region"],
            ["NO", "Norway", "Karl Johans gate 22", "0159", "Oslo", "Oslo County"],
            ["PL", "Poland", "Nowy Świat 33", "00-029", "Warsaw", "Masovian Voivodeship"],
            ["PT", "Portugal", "Avenida da Liberdade 190", "1250-147", "Lisbon", "Lisbon District"],
            ["RO", "Romania", "Bulevardul Unirii 10", "030119", "Bucharest", "Bucharest"],
            ["RU", "Russia", "Tverskaya Street 7", "125009", "Moscow", "Moscow"],
            ["SM", "San Marino", "Via Giacomo Matteotti 22", "47890", "San Marino", "San Marino"],
            ["RS", "Serbia", "Knez Mihailova 18", "11000", "Belgrade", "Belgrade"],
            ["SK", "Slovakia", "Obchodná 10", "81106", "Bratislava", "Bratislava Region"],
            ["SI", "Slovenia", "Slovenska cesta 5", "1000", "Ljubljana", "Ljubljana"],
            ["ES", "Spain", "Gran Vía 32", "28013", "Madrid", "Madrid"],
            ["SE", "Sweden", "Drottninggatan 42", "11151", "Stockholm", "Stockholm County"],
            ["CH", "Switzerland", "Bahnhofstrasse 50", "8001", "Zürich", "Zürich"],
            ["UA", "Ukraine", "Khreshchatyk Street 22", "01001", "Kyiv", "Kyiv"],
            ["GB", "United Kingdom", "Oxford Street 120", "W1D 1LT", "London", "England"],
            ["VA", "Vatican City", "Via della Conciliazione 4", "00120", "Vatican City", "Vatican City"]
        ],
        # Countries of North America
        2: [
            ["CA", "Canada", "123 Maple Leaf Rd", "M4B 1B3", "Toronto", "Ontario"],
            ["US", "United States", "456 Liberty St", "10001", "New York", "New York"],
            ["MX", "Mexico", "789 Avenida Reforma", "06000", "Mexico City", "Ciudad de México"]
        ],
        # Countries of South America
        3: [
            ["AR", "Argentina", "742 Avenida de Mayo", "C1084", "Buenos Aires", "Buenos Aires"],
            ["BO", "Bolivia", "123 Calle 21", "SCZ-001", "Santa Cruz", "Santa Cruz"],
            ["BR", "Brazil", "456 Avenida Paulista", "01310-000", "São Paulo", "São Paulo"],
            ["CL", "Chile", "789 Avenida Libertador", "8320000", "Santiago", "Región Metropolitana"],
            ["CO", "Colombia", "101 Calle 26", "110911", "Bogotá", "Bogotá D.C."],
            ["EC", "Ecuador", "234 Avenida Amazonas", "170150", "Quito", "Pichincha"],
            ["GY", "Guyana", "345 Brickdam Street", "GE-001", "Georgetown", "Demerara-Mahaica"],
            ["PY", "Paraguay", "567 Calle Palma", "1209", "Asunción", "Asunción"],
            ["PE", "Peru", "678 Avenida Arequipa", "15046", "Lima", "Lima"],
            ["SR", "Suriname", "789 Waterkant", "SR-001", "Paramaribo", "Paramaribo"],
            ["UY", "Uruguay", "890 Avenida 18 de Julio", "11200", "Montevideo", "Montevideo"],
            ["VE", "Venezuela", "901 Avenida Bolívar", "1010", "Caracas", "Distrito Capital"]
        ],
        # Countries of Central America
        4: [
            ["BZ", "Belize", "100 Regent Street", "BZ-001", "Belize City", "Belize District"],
            ["CR", "Costa Rica", "200 Avenida Central", "10101", "San José", "San José Province"],
            ["SV", "El Salvador", "300 Calle Arce", "01101", "San Salvador", "San Salvador Department"],
            ["GT", "Guatemala", "400 Avenida Reforma", "01010", "Guatemala City", "Guatemala Department"],
            ["HN", "Honduras", "500 Boulevard Morazán", "11101", "Tegucigalpa", "Francisco Morazán"],
            ["NI", "Nicaragua", "600 Carretera Masaya", "14001", "Managua", "Managua Department"],
            ["PA", "Panama", "700 Calle 50", "0801", "Panama City", "Panamá Province"]
        ],
        # Countries of Asia
        5: [
            ["AF", "Afghanistan", "100 Darulaman Road", "1001", "Kabul", "Kabul Province"],
            ["AZ", "Azerbaijan", "200 Azadliq Avenue", "AZ1000", "Baku", "Absheron"],
            ["BH", "Bahrain", "300 Government Avenue", "199", "Manama", "Capital Governorate"],
            ["BD", "Bangladesh", "400 Shahbagh Road", "1207", "Dhaka", "Dhaka Division"],
            ["BT", "Bhutan", "500 Norzin Lam", "11001", "Thimphu", "Thimphu District"],
            ["BN", "Brunei", "600 Jalan Sultan", "BS8611", "Bandar Seri Begawan", "Brunei-Muara"],
            ["KH", "Cambodia", "700 Monivong Blvd", "12000", "Phnom Penh", "Phnom Penh Municipality"],
            ["CN", "China", "800 Chang''an Avenue", "100000", "Beijing", "Beijing Municipality"],
            ["IN", "India", "900 Sansad Marg", "110001", "New Delhi", "Delhi"],
            ["ID", "Indonesia", "1000 Jalan Medan Merdeka", "10110", "Jakarta", "DKI Jakarta"],
            ["IR", "Iran", "1100 Azadi Street", "13185", "Tehran", "Tehran Province"],
            ["IQ", "Iraq", "1200 Karrada Street", "10011", "Baghdad", "Baghdad Governorate"],
            ["IL", "Israel", "1300 Kaplan Street", "61000", "Tel Aviv", "Tel Aviv District"],
            ["JP", "Japan", "1400 Chiyoda", "100-0001", "Tokyo", "Tokyo Prefecture"],
            ["JO", "Jordan", "1500 Zahran Street", "11118", "Amman", "Amman Governorate"],
            ["KZ", "Kazakhstan", "1600 Abay Avenue", "010000", "Astana", "Akmola Region"],
            ["KW", "Kuwait", "1700 Gulf Street", "13001", "Kuwait City", "Capital Governorate"],
            ["KG", "Kyrgyzstan", "1800 Chuy Avenue", "720001", "Bishkek", "Chuy Region"],
            ["LA", "Laos", "1900 Setthathirath Road", "01000", "Vientiane", "Vientiane Prefecture"],
            ["LB", "Lebanon", "2000 Hamra Street", "1103", "Beirut", "Beirut Governorate"],
            ["MY", "Malaysia", "2100 Jalan Parliament", "50502", "Kuala Lumpur", "Federal Territory"],
            ["MV", "Maldives", "2200 Ameenee Magu", "20026", "Malé", "Kaafu Atoll"],
            ["MN", "Mongolia", "2300 Peace Avenue", "14210", "Ulaanbaatar", "Ulaanbaatar Municipality"],
            ["MM", "Myanmar", "2400 Pyay Road", "11041", "Naypyidaw", "Naypyidaw Union Territory"],
            ["NP", "Nepal", "2500 Singh Durbar", "44600", "Kathmandu", "Bagmati Province"],
            ["KP", "North Korea", "2600 Kwangbok Street", "999093", "Pyongyang", "Pyongyang Direct Admin"],
            ["OM", "Oman", "2700 Al Kharjiyah Street", "112", "Muscat", "Muscat Governorate"],
            ["PK", "Pakistan", "2800 Constitution Avenue", "44000", "Islamabad", "Islamabad Capital Territory"],
            ["PS", "Palestine", "2900 Al Ersal Street", "00970", "Ramallah", "West Bank"],
            ["PH", "Philippines", "3000 Roxas Boulevard", "1000", "Manila", "National Capital Region"],
            ["QA", "Qatar", "3100 Corniche Road", "122104", "Doha", "Doha Municipality"],
            ["SA", "Saudi Arabia", "3200 Olaya Street", "11564", "Riyadh", "Riyadh Province"],
            ["SG", "Singapore", "3300 Orchard Road", "238896", "Singapore", "Central Region"],
            ["KR", "South Korea", "3400 Sejong-daero", "04524", "Seoul", "Seoul Capital Area"],
            ["LK", "Sri Lanka", "3500 Galle Road", "00300", "Colombo", "Western Province"],
            ["SY", "Syria", "3600 Mezzeh Autostrade", "11000", "Damascus", "Damascus Governorate"],
            ["TW", "Taiwan", "3700 Ren’ai Road", "100", "Taipei", "Taipei City"],
            ["TJ", "Tajikistan", "3800 Rudaki Avenue", "734025", "Dushanbe", "Dushanbe City"],
            ["TH", "Thailand", "3900 Ratchadamnoen Avenue", "10200", "Bangkok", "Bangkok Metropolitan"],
            ["TL", "Timor-Leste", "4000 Avenida Presidente Nicolau Lobato", "67000", "Dili", "Dili District"],
            ["TR", "Turkey", "4100 Atatürk Boulevard", "06570", "Ankara", "Ankara Province"],
            ["TM", "Turkmenistan", "4200 Archabil Avenue", "744000", "Ashgabat", "Ashgabat City"],
            ["AE", "United Arab Emirates", "4300 Sheikh Zayed Road", "00000", "Abu Dhabi", "Abu Dhabi Emirate"],
            ["UZ", "Uzbekistan", "4400 Amir Temur Avenue", "100000", "Tashkent", "Tashkent City"],
            ["VN", "Vietnam", "4500 Le Duan Boulevard", "100000", "Hanoi", "Hanoi Municipality"],
            ["YE", "Yemen", "4600 Zubayri Street", "00967", "Sana''a", "Sana''a Governorate"]
        ],
        # Countries of Africa
        6: [
            ["DZ", "Algeria", "100 Didouche Mourad St", "16000", "Algiers", "Algiers Province"],
            ["AO", "Angola", "200 Rua Major Kanhangulo", "1000", "Luanda", "Luanda Province"],
            ["BJ", "Benin", "300 Boulevard de la Marina", "01BP", "Porto-Novo", "Ouémé Department"],
            ["BW", "Botswana", "400 Independence Avenue", "0000", "Gaborone", "South-East District"],
            ["BF", "Burkina Faso", "500 Avenue de l’Indépendance", "01BP", "Ouagadougou", "Centre Region"],
            ["BI", "Burundi", "600 Boulevard de l’Uprona", "B.P.", "Gitega", "Gitega Province"],
            ["CM", "Cameroon", "700 Rue de la République", "BP", "Yaoundé", "Centre Region"],
            ["CV", "Cape Verde", "800 Avenida Amílcar Cabral", "7600", "Praia", "Santiago Island"],
            ["CF", "Central African Republic", "900 Rue Boganda", "BP", "Bangui", "Ombella-M''Poko"],
            ["TD", "Chad", "1000 Avenue Charles de Gaulle", "BP", "N''Djamena", "Chari-Baguirmi"],
            ["KM", "Comoros", "1100 Avenue de la République", "BP", "Moroni", "Grande Comore"],
            ["CG", "Congo (Brazzaville)", "1200 Avenue des Trois Martyrs", "BP", "Brazzaville", "Brazzaville District"],
            ["CD", "Congo (Kinshasa)", "1300 Boulevard du 30 Juin", "BP", "Kinshasa", "Kinshasa City-Province"],
            ["CI", "Côte d''Ivoire", "1400 Rue Lepic", "01BP", "Yamoussoukro", "Lacs District"],
            ["DJ", "Djibouti", "1500 Rue de Bender", "BP", "Djibouti", "Djibouti Region"],
            ["EG", "Egypt", "1600 Tahrir Square", "11511", "Cairo", "Cairo Governorate"],
            ["GQ", "Equatorial Guinea", "1700 Avenida Hassan II", "BP", "Malabo", "Bioko Norte"],
            ["ER", "Eritrea", "1800 Sematat Avenue", "P.O. Box", "Asmara", "Central Region"],
            ["SZ", "Eswatini", "1900 Gwamile Street", "H100", "Mbabane", "Hhohho Region"],
            ["ET", "Ethiopia", "2000 Churchill Avenue", "1000", "Addis Ababa", "Addis Ababa City"],
            ["GA", "Gabon", "2100 Boulevard Triomphal", "BP", "Libreville", "Estuaire Province"],
            ["GM", "Gambia", "2200 Independence Drive", "Banjul", "Banjul", "Banjul Region"],
            ["GH", "Ghana", "2300 Castle Road", "GA000", "Accra", "Greater Accra Region"],
            ["GN", "Guinea", "2400 8th Boulevard", "BP", "Conakry", "Conakry Region"],
            ["GW", "Guinea-Bissau", "2500 Avenida Amílcar Cabral", "BP", "Bissau", "Bissau Autonomous Sector"],
            ["KE", "Kenya", "2600 Harambee Avenue", "00100", "Nairobi", "Nairobi County"],
            ["LS", "Lesotho", "2700 Kingsway Street", "100", "Maseru", "Maseru District"],
            ["LR", "Liberia", "2800 Broad Street", "1000", "Monrovia", "Montserrado County"],
            ["LY", "Libya", "2900 Shara Zawiyat Dahmani", "12345", "Tripoli", "Tripoli District"],
            ["MG", "Madagascar", "3000 Avenue de l’Indépendance", "101", "Antananarivo", "Analamanga Region"],
            ["MW", "Malawi", "3100 Presidential Way", "101", "Lilongwe", "Central Region"],
            ["ML", "Mali", "3200 Avenue de l’OUA", "BP", "Bamako", "Bamako District"],
            ["MR", "Mauritania", "3300 Rue Mamadou Konaté", "BP", "Nouakchott", "Nouakchott-Ouest"],
            ["MU", "Mauritius", "3400 Government House St", "0000", "Port Louis", "Port Louis District"],
            ["MA", "Morocco", "3500 Avenue Hassan II", "10000", "Rabat", "Rabat-Salé-Kénitra"],
            ["MZ", "Mozambique", "3600 Avenida 25 de Setembro", "1100", "Maputo", "Maputo City"],
            ["NA", "Namibia", "3700 Robert Mugabe Ave", "9000", "Windhoek", "Khomas Region"],
            ["NE", "Niger", "3800 Rue de la Liberté", "BP", "Niamey", "Niamey Urban Community"],
            ["NG", "Nigeria", "3900 Tafawa Balewa Way", "900001", "Abuja", "Federal Capital Territory"],
            ["RW", "Rwanda", "4000 Boulevard de l’Armée", "PO Box", "Kigali", "Kigali City"],
            ["ST", "Sao Tome and Principe", "4100 Avenida Marginal 12 de Julho", "BP", "São Tomé", "Água Grande District"],
            ["SN", "Senegal", "4200 Avenue Léopold Sédar Senghor", "BP", "Dakar", "Dakar Region"],
            ["SC", "Seychelles", "4300 Independence Avenue", "000", "Victoria", "Mahé Island"],
            ["SL", "Sierra Leone", "4400 State Avenue", "Freetown", "Freetown", "Western Area"],
            ["SO", "Somalia", "4500 Maka Al Mukarama Rd", "252", "Mogadishu", "Banaadir Region"],
            ["ZA", "South Africa", "4600 Government Avenue", "0002", "Pretoria", "Gauteng Province"],
            ["SS", "South Sudan", "4700 Ministries Road", "21100", "Juba", "Central Equatoria"],
            ["SD", "Sudan", "4800 Nile Street", "11111", "Khartoum", "Khartoum State"],
            ["TZ", "Tanzania", "4900 Ohio Street", "11101", "Dodoma", "Dodoma Region"],
            ["TG", "Togo", "5000 Rue de l’Indépendance", "BP", "Lomé", "Maritime Region"],
            ["TN", "Tunisia", "5100 Avenue Habib Bourguiba", "1000", "Tunis", "Tunis Governorate"],
            ["UG", "Uganda", "5200 Parliamentary Avenue", "256", "Kampala", "Central Region"],
            ["ZM", "Zambia", "5300 Independence Avenue", "10101", "Lusaka", "Lusaka Province"],
            ["ZW", "Zimbabwe", "5400 Samora Machel Avenue", "00263", "Harare", "Harare Province"]
        ],
        # Countries of Oceania
        7: [
            ["AS", "American Samoa", "1000 Pago Pago Harbor Rd", "96799", "Pago Pago", "Tutuila"],
            ["AU", "Australia", "2000 George St", "2000", "Sydney", "New South Wales"],
            ["CK", "Cook Islands", "3000 Avarua Main Rd", "K3", "Avarua", "Rarotonga"],
            ["FJ", "Fiji", "4000 Victoria Parade", "0100", "Suva", "Central Division"],
            ["PF", "French Polynesia", "5000 Rue de l’Aéroport", "98713", "Papeete", "Tahiti"],
            ["GU", "Guam", "6000 Marine Corps Dr", "96910", "Hagåtña", "Guam"],
            ["KI", "Kiribati", "7000 Bairiki Rd", "KH200", "Tarawa", "Gilbert Islands"],
            ["MH", "Marshall Islands", "8000 Delap Rd", "96960", "Majuro", "Majuro Atoll"],
            ["FM", "Micronesia (Federated States of)", "9000 Pohnpei Ave", "96941", "Palikir", "Pohnpei State"],
            ["NR", "Nauru", "10000 Yaren St", "NA001", "Yaren", "Yaren District"],
            ["NC", "New Caledonia", "11000 Rue de la République", "98800", "Nouméa", "South Province"],
            ["NZ", "New Zealand", "12000 Victoria St", "6011", "Wellington", "Wellington Region"],
            ["NU", "Niue", "13000 Alofi North Rd", "NIU", "Alofi", "Niue"],
            ["NF", "Norfolk Island", "14000 New Cascade Rd", "2899", "Kingston", "Norfolk Island"],
            ["MP", "Northern Mariana Islands", "15000 Beach Rd", "96950", "Saipan", "Saipan"],
            ["PW", "Palau", "16000 Malakal Dr", "96940", "Ngerulmud", "Melekeok"],
            ["PG", "Papua New Guinea", "17000 Waigani Dr", "121", "Port Moresby", "National Capital District"],
            ["PN", "Pitcairn Islands", "18000 Adamstown Rd", "PN01", "Adamstown", "Pitcairn Island"],
            ["WS", "Samoa", "19000 Apia Park Rd", "1100", "Apia", "Upolu"],
            ["SB", "Solomon Islands", "20000 Honiara St", "SBI100", "Honiara", "Guadalcanal"],
            ["TK", "Tokelau", "21000 Atafu Rd", "TK100", "Atafu", "Atafu Atoll"],
            ["TO", "Tonga", "22000 Queen Salote Rd", "TGA", "Nuku’alofa", "Tongatapu"],
            ["TV", "Tuvalu", "23000 Funafuti St", "TV01", "Funafuti", "Funafuti Atoll"],
            ["VU", "Vanuatu", "24000 Lini Hwy", "100", "Port Vila", "Shefa"],
            ["WF", "Wallis and Futuna", "25000 Rue de l''Administration", "98600", "Mata-Utu", "Wallis Island"]
        ],
        # Countries of the Caribbean
        8: [
            ["AG", "Antigua and Barbuda", "1000 St. John''s Rd", "001", "St. John''s", "Saint John"],
            ["BS", "Bahamas", "2000 Bay St", "N/A", "Nassau", "New Providence"],
            ["BB", "Barbados", "3000 Broad St", "BB12345", "Bridgetown", "Saint Michael"],
            ["CU", "Cuba", "4000 Calle 23", "10400", "Havana", "Havana"],
            ["DM", "Dominica", "5000 Victoria St", "DM1", "Roseau", "Saint George"],
            ["DO", "Dominican Republic", "6000 Av. Winston Churchill", "10100", "Santo Domingo", "Distrito Nacional"],
            ["HT", "Haiti", "7000 Rue des Fronts Forts", "HT6110", "Port-au-Prince", "Ouest"],
            ["JM", "Jamaica", "8000 Half Way Tree Rd", "JAM001", "Kingston", "Kingston Parish"],
            ["KN", "Saint Kitts and Nevis", "9000 Fort Street", "VC6", "Basseterre", "Saint George"],
            ["LC", "Saint Lucia", "10000 Micoud St", "LC123", "Castries", "Castries"],
            ["VC", "Saint Vincent and the Grenadines", "11000 Main St", "VC1", "Kingstown", "Saint George"],
            ["GD", "Grenada", "12000 Grand Anse Rd", "GRD01", "St. George''s", "Saint George"],
            ["TT", "Trinidad and Tobago", "13000 Charlotte St", "TTO100", "Port of Spain", "Trinidad"],
            ["AW", "Aruba", "14000 Wilhelminastraat", "AW01", "Oranjestad", "Aruba"],
            ["BQ", "Bonaire, Sint Eustatius and Saba", "15000 Kralendijk", "BES100", "Kralendijk", "Bonaire"],
            ["GG", "Guadeloupe", "16000 Rue de la République", "97100", "Basse-Terre", "Guadeloupe"],
            ["MQ", "Martinique", "17000 Rue Schœlcher", "97200", "Fort-de-France", "Martinique"],
            ["PR", "Puerto Rico", "18000 Ave. Constitution", "00901", "San Juan", "San Juan"],
            ["RE", "Réunion", "19000 Rue de la Victoire", "97400", "Saint-Denis", "Réunion"],
            ["SX", "Sint Maarten", "20000 Front St", "SX100", "Philipsburg", "Sint Maarten"],
            ["BL", "Saint Barthélemy", "21000 Rue de Lorient", "97133", "Gustavia", "Saint Barthélemy"],
            ["MF", "Saint Martin", "22000 Rue de la République", "97150", "Marigot", "Saint Martin"],
            ["CW", "Curaçao", "23000 Willemstad Rd", "CW100", "Willemstad", "Curaçao"],
            ["VG", "British Virgin Islands", "24000 Road Town", "VGB100", "Road Town", "Tortola"],
            ["VI", "United States Virgin Islands", "25000 St. Thomas Rd", "00802", "Charlotte Amalie", "Saint Thomas"]
        ]
    }

    department_list = [
        "Executive"                ,
        "Administration"           ,
        "Marketing"                ,
        "Purchasing"               ,
        "Human Resources"          ,
        "Shipping"                 ,
        "IT"                       ,
        "Public Relations"         ,
        "Sales"                    ,
        "Finance"                  ,
        "Accounting"               ,
        "Manufacturing"            ,
        "Research"                 ,
        "Customer Service"         ,
        "Legal"                    ,
        "Quality Assurance"        ,
        "Support"                  ,
        "Training"                 ,
        "Logistics"                ,
        "Security"                 ,
        "Facilities"               ,
        "Maintenance"              ,
        "Engineering"              ,
        "Design"                   ,
        "Product Development"
    ]

    jobs_list = [
        ["President", 20000.00, 40000.00],
        ["Vice President", 15000.00, 30000.00],
        ["Assistant Vice President", 10000.00, 20000.00],
        ["Manager", 8000.00, 15000.00],
        ["Assistant Manager", 6000.00, 12000.00],
        ["Supervisor", 5000.00, 10000.00],
        ["Team Leader", 4500.00, 9000.00],
        ["Senior Software Engineer", 6000.00, 12000.00],
        ["Software Engineer", 4000.00, 8000.00],
        ["Junior Software Engineer", 2500.00, 5000.00],
        ["Senior Systems Analyst", 5500.00, 11000.00],
        ["Systems Analyst", 3500.00, 7000.00],
        ["Senior Accountant", 4500.00, 9000.00],
        ["Accountant", 3000.00, 6000.00],
        ["Marketing Director", 8000.00, 16000.00],
        ["Marketing Manager", 6000.00, 12000.00],
        ["Marketing Specialist", 3500.00, 7000.00],
        ["Sales Director", 8000.00, 16000.00],
        ["Sales Manager", 6000.00, 12000.00],
        ["Sales Representative", 2500.00, 5000.00],
        ["HR Director", 8000.00, 16000.00],
        ["HR Manager", 6000.00, 12000.00],
        ["HR Specialist", 3500.00, 7000.00],
        ["Administrative Assistant", 2000.00, 4000.00],
        ["Executive Assistant", 3000.00, 6000.00],
        ["Receptionist", 1500.00, 3000.00],
        ["Customer Service Manager", 4000.00, 8000.00],
        ["Customer Service Representative", 2000.00, 4000.00],
        ["IT Manager", 6000.00, 12000.00],
        ["Network Administrator", 4000.00, 8000.00],
        ["Database Administrator", 4500.00, 9000.00],
        ["Web Developer", 3500.00, 7000.00],
        ["Graphic Designer", 3000.00, 6000.00],
        ["Content Writer", 2500.00, 5000.00],
        ["SEO Specialist", 3000.00, 6000.00],
        ["Data Analyst", 4000.00, 8000.00],
        ["Business Analyst", 4500.00, 9000.00],
        ["Project Manager", 6000.00, 12000.00],
        ["Quality Assurance Engineer", 3500.00, 7000.00],
        ["Technical Support Specialist", 2500.00, 5000.00],
        ["Training Coordinator", 3000.00, 6000.00],
        ["Logistics Coordinator", 3500.00, 7000.00],
        ["Security Officer", 2000.00, 4000.00],
        ["Facilities Manager", 5000.00, 10000.00],
        ["Maintenance Technician", 2500.00, 5000.00],
        ["Engineering Manager", 7000.00, 14000.00],
        ["Design Engineer", 4000.00, 8000.00],
        ["Product Manager", 6000.00, 12000.00],
        ["Research Scientist", 5000.00, 10000.00],
        ["Legal Counsel", 6000.00, 12000.00],
        ["Paralegal", 3000.00, 6000.00],
        ["Compliance Officer", 4000.00, 8000.00],
        ["Risk Manager", 5000.00, 10000.00],
        ["Insurance Underwriter", 3500.00, 7000.00],
        ["Claims Adjuster", 3000.00, 6000.00],
        ["Financial Analyst", 4000.00, 8000.00],
        ["Investment Analyst", 5000.00, 10000.00],
        ["Tax Specialist", 3500.00, 7000.00],
        ["Payroll Specialist", 3000.00, 6000.00],
        ["Cost Accountant", 3500.00, 7000.00],
        ["Budget Analyst", 4000.00, 8000.00],
        ["Internal Auditor", 4500.00, 9000.00],
        ["External Auditor", 5000.00, 10000.00],
        ["Financial Controller", 6000.00, 12000.00],
        ["Chief Financial Officer", 8000.00, 16000.00]
    ]

    name_list = [
        ["John Doe",            "555-1234",   55000.00   ],
        ["Jane Smith",          "555-5678",   60000.00   ],
        ["Alice Johnson",       "555-8765",   70000.00   ],
        ["Bob Brown",           "555-4321",   80000.00   ],
        ["Charlie Davis",       "555-6789",   90000.00   ],
        ["Diana Prince",        "555-3456",   100000.00  ],
        ["Ethan Hunt",          "555-7890",   110000.00  ],
        ["Fiona Apple",         "555-2345",   120000.00  ],
        ["George Clooney",      "555-0987",   130000.00  ],
        ["Hannah Montana",      "555-6543",   140000.00  ],
        ["Ian Malcolm",         "555-3210",   150000.00  ],
        ["Jack Sparrow",        "555-4567",   160000.00  ],
        ["Katherine Johnson",   "555-8901",   170000.00  ],
        ["Leonardo DiCaprio",   "555-2346",   180000.00  ],
        ["Mia Wallace",         "555-6780",   190000.00  ],
        ["Nina Simone",         "555-7891",   200000.00  ],
        ["Oscar Isaac",         "555-8902",   210000.00  ],
        ["Paula Patton",        "555-9012",   220000.00  ],
        ["Quentin Tarantino",   "555-0123",   230000.00  ],
        ["Rihanna Black",       "555-1235",   240000.00  ],
        ["Steve Jobs",          "555-2347",   250000.00  ],
        ["Tina Fey",            "555-3458",   260000.00  ],
        ["Uma Thurman",         "555-4569",   270000.00  ],
        ["Vin Diesel",          "555-5670",   280000.00  ],
        ["Will Ferrell",        "555-6781",   290000.00  ],
        ["Xander Cage",         "555-7892",   300000.00  ],
        ["Samuel Jackson",      "555-8903",   310000.00  ],
        ["Tom Hanks",           "555-9014",   320000.00  ],
        ["Vera Farmiga",        "555-1236",   340000.00  ],
        ["Will Smith",          "555-2347",   350000.00  ],
        ["Xena Warrior",        "555-3458",   360000.00  ],
        ["Yoda Skywalker",      "555-4569",   370000.00  ],
        ["Zoe Saldana",         "555-5670",   380000.00  ],
        ["Aaron Paul",          "555-6781",   390000.00  ],
        ["Brad Pitt",           "555-7892",   400000.00  ],
        ["Cameron Diaz",        "555-8903",   410000.00  ],
        ["Dwayne Johnson",      "555-9014",   420000.00  ],
        ["Eva Mendes",          "555-0125",   430000.00  ],
        ["Freddie Mercury",     "555-1236",   440000.00  ],
        ["Gwen Stefani",        "555-2347",   450000.00  ],
        ["Hugh Jackman",        "555-3458",   460000.00  ],
        ["Isla Fisher",         "555-4569",   470000.00  ],
        ["Jared Leto",          "555-5670",   480000.00  ],
        ["Kate Winslet",        "555-6781",   490000.00  ],
        ["Matthew McConaughey", "555-8903",   510000.00  ],
        ["Natalie Portman",     "555-9014",   520000.00  ],
        ["Olivia Wilde",        "555-0125",   530000.00  ],
        ["Paul Rudd",           "555-1236",   540000.00  ],
        ["Queen Latifah",       "555-2347",   550000.00  ],
        ["Ryan Reynolds",       "555-3458",   560000.00  ],
        ["Scarlett Johansson",  "555-4569",   570000.00  ],
        ["Tom Hardy",           "555-5670",   580000.00  ]
    ]

    dependent_list = [
        "Alex Rodriguez",
        "Betty White",
        "Chris Evans",
        "David Attenborough",
        "Emma Stone",
        "Frank Sinatra",
        "Grace Kelly",
        "Hugh Jackman",
        "Iris Apfel",
        "James Dean",
        "Kate Winslet",
        "Louis Armstrong",
        "Meryl Streep",
        "Neil Armstrong",
        "Oprah Winfrey",
        "Paul Newman",
        "Queen Latifah",
        "Robert Redford",
        "Susan Sarandon",
        "Tom Cruise",
        "Albert Einstein",
        "Marie Curie",
        "Isaac Newton",
        "Charles Darwin",
        "Nikola Tesla",
        "Galileo Galilei",
        "Ada Lovelace",
        "Stephen Hawking",
        "Grace Hopper",
        "Richard Feynman",
        "Rachel Carson",
        "Alan Turing",
        "Dorothy Hodgkin",
        "Max Planck",
        "Rosalind Franklin",
        "Werner Heisenberg",
        "Barbara McClintock",
        "Niels Bohr",
        "Emmy Noether",
        "Carl Sagan",
        "Jane Goodall",
        "Paul Dirac",
        "Lise Meitner",
        "James Maxwell",
        "Katherine Johnson",
        "Michael Faraday",
        "Rita Levi-Montalcini",
        "Louis Pasteur",
        "Maria Mayer",
        "David Hilbert"
    ]

    queries = []
    queries.extend(generate_begin_instructions())
    queries.extend(generate_tables_create())
    queries.extend(generate_regions_insert_instructions(regions_list))
    queries.extend(generate_countries_insert_instructions(regions_list, region_countries_list))
    queries.extend(generate_locations_insert_instructions(regions_list, region_countries_list))
    queries.extend(generate_departments_insert_instructions(department_list))
    queries.extend(generate_jobs_insert_instructions(jobs_list))

    iter = 100

    for i in range(iter):
        random_insert_count = random.randint(1, 5)
        random_delete_count = random.randint(0, random_insert_count - 1)

        # generate random employee insert statements
        for i in range(random_insert_count):
            queries.extend(generate_insert_employee_statement(name_list, dependent_list))

        # generate random delete employee statements
        for i in range(random_delete_count):
            queries.extend(generate_delete_employee_statement())

        # generate random dependent insert statements
        for i in range(random_insert_count):
            queries.extend(generate_insert_dependent_statements(dependent_list))

        # generate random dependent delete statements
        for i in range(random_delete_count):
            queries.extend(generate_delete_dependent_statement())

        # generate random salary update statements
        random_update_count = random.randint(1, len(current_employees))
        for i in range(random_update_count):
            queries.extend(generate_update_salary_statement(10000))

    queries.extend(generate_end_instructions())

    for q in queries:
        print(q)

