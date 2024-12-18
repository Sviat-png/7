import psycopg2
from psycopg2 import sql

# Параметри підключення до бази даних
connection_params = {
    "dbname": "db",
    "user": "user",
    "password": "password",
    "host": "localhost",
    "port": 5432
}

# Клас для роботи з базою даних
class Database:
    def __init__(self, connection_params):
        self.connection_params = connection_params

    def connect(self):
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            self.cursor = self.conn.cursor()
            print("Підключено до бази даних.")
        except Exception as e:
            print("Помилка підключення:", e)

    def close(self):
        self.cursor.close()
        self.conn.close()

    def execute_query(self, query, params=None, fetch=False):
        try:
            self.cursor.execute(query, params)
            self.conn.commit()
            if fetch:
                return self.cursor.fetchall()
        except Exception as e:
            print("Помилка виконання запиту:", e)
            self.conn.rollback()

    def create_tables(self):
        # Створення таблиць
        self.execute_query("""
            CREATE TABLE IF NOT EXISTS locomotives (
                reg_number SERIAL PRIMARY KEY,
                depot VARCHAR(50) NOT NULL,
                type VARCHAR(50) NOT NULL,
                year INT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS repairs (
                repair_id SERIAL PRIMARY KEY,
                reg_number INT REFERENCES locomotives(reg_number) ON DELETE CASCADE,
                repair_type VARCHAR(50) NOT NULL,
                start_date DATE NOT NULL,
                days_needed INT NOT NULL,
                daily_cost NUMERIC(10, 2) NOT NULL,
                team_number INT
            );
            CREATE TABLE IF NOT EXISTS teams (
                team_number SERIAL PRIMARY KEY,
                phone VARCHAR(20) NOT NULL
            );
            CREATE TABLE IF NOT EXISTS workers (
                worker_id SERIAL PRIMARY KEY,
                last_name VARCHAR(50) NOT NULL,
                first_name VARCHAR(50) NOT NULL,
                middle_name VARCHAR(50) NOT NULL,
                team_number INT REFERENCES teams(team_number) ON DELETE CASCADE,
                is_leader BOOLEAN NOT NULL,
                birth_date DATE NOT NULL
            );
        """)
        print("Таблиці успішно створені.")

    def insert_sample_data(self):
        locomotives_data = [
            ("Фастів", "вантажний", 2010),
            ("Козятин", "пасажирський", 2015),
            ("П’ятихатки", "вантажний", 2008)
        ]
        repairs_data = [
            (1, "поточний", "2024-01-01", 5, 1500.00, 1),
            (2, "технічне обслуговування", "2024-01-05", 3, 2000.00, 2),
            (3, "позаплановий", "2024-01-10", 7, 1800.00, 3)
        ]
        teams_data = [
            ("123-456-7890"),
            ("987-654-3210"),
            ("555-555-5555")
        ]
        workers_data = [
            ("Іваненко", "Іван", "Іванович", 1, True, "1980-05-15"),
            ("Петренко", "Петро", "Петрович", 1, False, "1990-06-20"),
            ("Сидоренко", "Сидір", "Сидорович", 2, False, "1985-07-30")
        ]

        for data in locomotives_data:
            self.execute_query("INSERT INTO locomotives (depot, type, year) VALUES (%s, %s, %s)", data)

        for data in repairs_data:
            self.execute_query("INSERT INTO repairs (reg_number, repair_type, start_date, days_needed, daily_cost, team_number) VALUES (%s, %s, %s, %s, %s, %s)", data)

        for data in teams_data:
            self.execute_query("INSERT INTO teams (phone) VALUES (%s)", (data,))

        for data in workers_data:
            self.execute_query("INSERT INTO workers (last_name, first_name, middle_name, team_number, is_leader, birth_date) VALUES (%s, %s, %s, %s, %s, %s)", data)

        print("Зразкові дані додані.")

    def display_freight_locomotives(self):
        result = self.execute_query("SELECT * FROM locomotives WHERE type = 'вантажний' ORDER BY year;", fetch=True)
        print("\nВантажні локомотиви:")
        for row in result:
            print(row)

    def calculate_repair_end_date(self):
        result = self.execute_query("""
            SELECT l.reg_number, r.start_date, r.start_date + r.days_needed * INTERVAL '1 day' AS end_date
            FROM repairs r
            JOIN locomotives l ON r.reg_number = l.reg_number;
        """, fetch=True)
        print("\nКінцеві дати ремонту:")
        for row in result:
            print(row)

    def count_repairs_per_team(self):
        result = self.execute_query("SELECT team_number, COUNT(*) FROM repairs GROUP BY team_number;", fetch=True)
        print("\nКількість ремонтів по бригадах:")
        for row in result:
            print(row)

    def calculate_repair_cost(self):
        result = self.execute_query("""
            SELECT l.reg_number, SUM(r.days_needed * r.daily_cost) AS total_cost
            FROM repairs r
            JOIN locomotives l ON r.reg_number = l.reg_number
            GROUP BY l.reg_number;
        """, fetch=True)
        print("\nПовна вартість ремонту для кожного локомотива:")
        for row in result:
            print(row)

    def count_repair_types_per_team(self):
        result = self.execute_query("""
            SELECT team_number, repair_type, COUNT(*) 
            FROM repairs 
            GROUP BY team_number, repair_type;
        """, fetch=True)
        print("\nКількість типів ремонтів по бригадах:")
        for row in result:
            print(row)

    def display_locomotives_by_depot(self, depot):
        result = self.execute_query("SELECT * FROM locomotives WHERE depot = %s;", (depot,), fetch=True)
        print(f"\nЛокомотиви депо '{depot}':")
        for row in result:
            print(row)

if __name__ == "__main__":
    db = Database(connection_params)
    db.connect()
    db.create_tables()
    db.insert_sample_data()
    db.display_freight_locomotives()
    db.calculate_repair_end_date()
    db.count_repairs_per_team()
    db.calculate_repair_cost()
    db.count_repair_types_per_team()
    db.display_locomotives_by_depot("Фастів")
    db.close()
