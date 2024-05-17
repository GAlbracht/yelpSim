import psycopg2
import json
import datetime
from decimal import Decimal

def connect_db():
    try:
        conn = psycopg2.connect(
            dbname="milestone1db",
            user="postgres",
            password="admin",
            host="localhost"
        )
        return conn
    except Exception as e:
        print(f"error: {e}")

def get_states(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT state FROM business ORDER BY state;")
        states = cur.fetchall()
        return states

def get_cities(conn, selected_state):
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT city FROM business WHERE state=%s ORDER BY city;", (selected_state,))
        cities = cur.fetchall()
        return cities

def get_businesses(conn, selected_city, selected_state):
    with conn.cursor() as cur:
        cur.execute("SELECT name, city, state FROM business WHERE city=%s AND state=%s ORDER BY name;", (selected_city, selected_state))
        businesses = cur.fetchall()
        return businesses

def get_zipcodes(conn, selected_city, selected_state):
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT postal_code FROM businesses WHERE city=%s AND state=%s ORDER BY postal_code;", (selected_city, selected_state))
        zipcodes = cur.fetchall()
        return zipcodes

def get_categories(conn, selected_zipcode):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT UNNEST(string_to_array(categories, ', ')) AS category
            FROM businesses
            WHERE postal_code=%s
            ORDER BY category;
        """, (selected_zipcode,))
        categories = cur.fetchall()
        return categories

def get_businesses_by_category(conn, selected_zipcode, selected_category):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT name, city, state, stars, review_count, reviewrating, "numCheckins",
            is_open, hours
            FROM businesses
            WHERE postal_code = %s AND categories LIKE %s
            ORDER BY name;
        """, (selected_zipcode, '%' + selected_category + '%',))
        businesses = cur.fetchall()
        return businesses
    
def calculate_success_score(last_review_date, avg_rating, numCheckins):
    today_date = datetime.datetime.now().date()
    days_since_last_review = (today_date - last_review_date).days
    business_age_years = days_since_last_review / 365.25  

    numCheckins = float(numCheckins)  
    avg_rating = float(avg_rating)  

    checkin_weight = 0.4
    review_weight = 0.2
    normalized_review_rating = avg_rating / 5

    success_score = (checkin_weight * (numCheckins / max(numCheckins, 1))) + (review_weight * normalized_review_rating)
    return success_score

def calculate_popularity_score(numCheckins, review_count):
    checkin_weight = 0.5
    review_weight = 0.5
    return (checkin_weight * numCheckins) + (review_weight * review_count)

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QComboBox, QVBoxLayout, QHBoxLayout,
    QWidget, QListWidget, QTableWidget, QTableWidgetItem, QLabel,
    QPushButton, QGroupBox, QGridLayout, QMessageBox
)

class MyApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.conn = connect_db() 
        self.setWindowTitle("Milestone 1")
        self.setGeometry(100, 100, 1400, 900)
        self.initUI()

    def initUI(self):
        mainLayout = QVBoxLayout()

        topRowLayout = QHBoxLayout()
        
        locationGroupBox = QGroupBox("Select Location")
        locationLayout = QGridLayout()
        locationLayout.addWidget(QLabel("State"), 0, 0)
        self.stateComboBox = QComboBox()
        locationLayout.addWidget(self.stateComboBox, 0, 1)
        locationLayout.addWidget(QLabel("City"), 1, 0)
        self.cityListWidget = QListWidget()
        self.cityListWidget.setSelectionMode(QListWidget.SingleSelection)  
        self.cityListWidget.itemSelectionChanged.connect(self.on_city_selected)
        locationLayout.addWidget(self.cityListWidget, 1, 1)
        locationLayout.addWidget(QLabel("Zip Code"), 2, 0)
        self.zipcodeListWidget = QListWidget()
        self.zipcodeListWidget.setSelectionMode(QListWidget.SingleSelection)
        self.zipcodeListWidget.itemSelectionChanged.connect(self.on_zipcode_selected)
        locationLayout.addWidget(self.zipcodeListWidget, 2, 1)
        locationGroupBox.setLayout(locationLayout)

        statsGroupBox = QGroupBox("Zipcode Statistics")
        statsLayout = QVBoxLayout()
        self.statsTable = QTableWidget()
        self.statsTable.setRowCount(1)
        self.statsTable.setColumnCount(3)
        self.statsTable.setHorizontalHeaderLabels(["# of Businesses", "Total Population", "Average Income"])
        self.statsTable.horizontalHeader().setStretchLastSection(True)
        statsLayout.addWidget(self.statsTable)
        statsGroupBox.setLayout(statsLayout)

        categoriesGroupBox = QGroupBox("Top Categories")
        categoriesLayout = QVBoxLayout()
        self.categoriesTable = QTableWidget()
        self.categoriesTable.setRowCount(1)
        self.categoriesTable.setColumnCount(2)
        self.categoriesTable.setHorizontalHeaderLabels(["Category", "# of Businesses"])
        categoriesLayout.addWidget(self.categoriesTable)
        categoriesGroupBox.setLayout(categoriesLayout)

        buttonLayout = QHBoxLayout()
        self.searchButton = QPushButton('Search')
        self.searchButton.clicked.connect(self.on_search_clicked)
        buttonLayout.addWidget(self.searchButton)

        self.refreshButton = QPushButton('Refresh')
        self.refreshButton.clicked.connect(self.on_refresh_clicked)
        buttonLayout.addWidget(self.refreshButton)

        topRowLayout.addWidget(locationGroupBox)
        topRowLayout.addWidget(statsGroupBox)
        topRowLayout.addWidget(categoriesGroupBox)
        topRowLayout.addWidget(self.searchButton)
        
        mainLayout.addLayout(topRowLayout)

        secondRowLayout = QHBoxLayout()

        centralWidget = QWidget()
        centralWidget.setLayout(mainLayout)
        self.setCentralWidget(centralWidget)

        filterGroupBox = QGroupBox("Filter on Categories")
        filterLayout = QVBoxLayout()
        self.filterListWidget = QListWidget()
        self.filterListWidget.itemSelectionChanged.connect(self.on_category_selected)

        filterLayout.addWidget(self.filterListWidget)
        filterGroupBox.setLayout(filterLayout)

        self.businessTable = QTableWidget(0, 6)
        self.businessTable.setHorizontalHeaderLabels([
            "Name", "City", "State", "Stars", "Review Count", "Review Rating"
        ])

        self.businessTable.setColumnWidth(0, 280)  
        self.businessTable.setColumnWidth(1, 100) 
        self.businessTable.setColumnWidth(2, 75)   
        self.businessTable.setColumnWidth(3, 55)  
        self.businessTable.setColumnWidth(4, 85)   
        self.businessTable.setColumnWidth(5, 85)   

        secondRowLayout.addWidget(filterGroupBox)
        secondRowLayout.addWidget(self.businessTable)

        mainLayout.addLayout(secondRowLayout)
        
        centralWidget = QWidget()
        centralWidget.setLayout(mainLayout)
        self.setCentralWidget(centralWidget)

        thirdRowLayout = QHBoxLayout()

        popularGroupBox = QGroupBox("Popular Businesses (in zipcode)")
        popularLayout = QVBoxLayout()
        self.popularBusinessTable = QTableWidget(0, 4)  
        self.popularBusinessTable.setHorizontalHeaderLabels([
            "Business Name", "Stars", "Review Count", "Popularity Score"
        ])
        popularLayout.addWidget(self.popularBusinessTable)
        popularGroupBox.setLayout(popularLayout)

        self.popularBusinessTable.setColumnWidth(0, 300)  
        self.popularBusinessTable.setColumnWidth(1, 100)  
        self.popularBusinessTable.setColumnWidth(2, 100)   
        self.popularBusinessTable.setColumnWidth(3, 100)

        successfulGroupBox = QGroupBox("Successful Businesses (in zipcode)")
        successfulLayout = QVBoxLayout()
        self.successfulBusinessTable = QTableWidget(0, 4)  
        self.successfulBusinessTable.setHorizontalHeaderLabels([
            "Business Name", "Review Count", "Number of Checkins", "Success Score"
        ])
        successfulLayout.addWidget(self.successfulBusinessTable)
        successfulGroupBox.setLayout(successfulLayout)

        self.successfulBusinessTable.setColumnWidth(0, 300)  
        self.successfulBusinessTable.setColumnWidth(1, 100)  
        self.successfulBusinessTable.setColumnWidth(2, 100)  
        self.successfulBusinessTable.setColumnWidth(3, 100)  

        thirdRowLayout.addWidget(popularGroupBox)
        thirdRowLayout.addWidget(successfulGroupBox)
        topRowLayout.addLayout(buttonLayout)

        mainLayout.addLayout(thirdRowLayout)
        
        centralWidget = QWidget()
        centralWidget.setLayout(mainLayout)
        self.setCentralWidget(centralWidget)

        self.load_states()

    def load_states(self):
        states = get_states(self.conn)
        for state in states:
            self.stateComboBox.addItem(state[0])
        self.stateComboBox.activated[str].connect(self.on_state_changed) 


    def on_state_changed(self, state):
        self.cityListWidget.clear()
        self.zipcodeListWidget.clear()
        self.filterListWidget.clear()
        self.businessTable.setRowCount(0)
        self.popularBusinessTable.setRowCount(0)
        self.successfulBusinessTable.setRowCount(0)
        self.categoriesTable.setRowCount(0)
        self.statsTable.setItem(0, 1, QTableWidgetItem("")) 
        self.statsTable.setItem(0, 2, QTableWidgetItem(""))  
        self.statsTable.setItem(0, 0, QTableWidgetItem(""))  

        self.businessTable.setRowCount(0)
        self.popularBusinessTable.setRowCount(0)
        self.successfulBusinessTable.setRowCount(0)

        cities = get_cities(self.conn, state)
        for city in cities:
            self.cityListWidget.addItem(city[0])

    def on_city_selected(self):
        selected_items = self.cityListWidget.selectedItems()
        if selected_items:
            selected_city = selected_items[0].text()
            state = self.stateComboBox.currentText()
            zipcodes = get_zipcodes(self.conn, selected_city, state)
            self.zipcodeListWidget.clear()
            self.filterListWidget.clear()
            self.businessTable.setRowCount(0)
            self.popularBusinessTable.setRowCount(0)
            self.successfulBusinessTable.setRowCount(0)
            for zipcode in zipcodes:
                self.zipcodeListWidget.addItem(zipcode[0])


    def load_businesses(self, city, state):
        self.businessTable.setRowCount(0)
        businesses = get_businesses(self.conn, city, state)
        for business in businesses:
            row_position = self.businessTable.rowCount()
            self.businessTable.insertRow(row_position)
            self.businessTable.setItem(row_position, 0, QTableWidgetItem(business[0]))
            self.businessTable.setItem(row_position, 1, QTableWidgetItem(business[1]))
            self.businessTable.setItem(row_position, 2, QTableWidgetItem(business[2]))

    def on_zipcode_selected(self):
        selected_items = self.zipcodeListWidget.selectedItems()
        if selected_items:
            selected_zipcode = selected_items[0].text()
            self.filterListWidget.clear()
            self.businessTable.setRowCount(0)
            self.popularBusinessTable.setRowCount(0)
            self.successfulBusinessTable.setRowCount(0)
            categories = get_categories(self.conn, selected_zipcode)
            for category in categories:
                self.filterListWidget.addItem(category[0])
            self.update_zipcode_stats(selected_zipcode)
            self.update_top_categories(selected_zipcode)


    def on_category_selected(self):
        selected_items = self.filterListWidget.selectedItems()
        if selected_items:
            selected_category = selected_items[0].text()
            self.businessTable.setRowCount(0)
            self.popularBusinessTable.setRowCount(0)
            self.successfulBusinessTable.setRowCount(0)

    def load_businesses_by_category(self, zipcode, category):
        businesses = get_businesses_by_category(self.conn, zipcode, category)
        self.businessTable.setRowCount(0)  
        for business in businesses:
            row_position = self.businessTable.rowCount()
            self.businessTable.insertRow(row_position)
            for i, property in enumerate(business):
                if i == 7:  
                    property = "Yes" if property else "No"
                elif i == 8:  
                    property = json.dumps(property) if isinstance(property, dict) else property
                self.businessTable.setItem(row_position, i, QTableWidgetItem(str(property)))

    def on_search_clicked(self):
        if not all([self.stateComboBox.currentText(), self.cityListWidget.currentItem(), 
                    self.zipcodeListWidget.currentItem(), self.filterListWidget.currentItem()]):
            QMessageBox.warning(self, "Incomplete Selection", "Please select all filters before searching.")
            return
        
        state = self.stateComboBox.currentText()
        city = self.cityListWidget.currentItem().text()
        zipcode = self.zipcodeListWidget.currentItem().text()
        category = self.filterListWidget.currentItem().text()

        self.load_businesses_by_category(zipcode, category)
        self.update_zipcode_stats(zipcode)
        self.update_top_categories(zipcode)
        self.update_popular_businesses(zipcode, category)
        self.update_successful_businesses(zipcode, category)

    def on_refresh_clicked(self):
        zipcode_item = self.zipcodeListWidget.currentItem()
        category_item = self.filterListWidget.currentItem()
        if zipcode_item and category_item:
            zipcode = zipcode_item.text()
            category = category_item.text()
            self.update_popular_businesses(zipcode, category)
            self.update_successful_businesses(zipcode, category)
        else:
            QMessageBox.warning(self, "Selection Incomplete", "Please select both a zipcode and a category to refresh.")
    
    def update_zipcode_stats(self, zipcode):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM businesses WHERE postal_code = %s;
            """, (zipcode,))
            business_count = cur.fetchone()[0]
            self.statsTable.setItem(0, 0, QTableWidgetItem(str(business_count)))  

            cur.execute("""
                SELECT population, avg_income FROM zipcodes WHERE zip_code = %s;
            """, (zipcode,))
            result = cur.fetchone()
            if result:
                population, avg_income = result
                self.statsTable.setItem(0, 1, QTableWidgetItem(str(population))) 
                self.statsTable.setItem(0, 2, QTableWidgetItem(f"{avg_income:,.1f}")) 
            else:
                self.statsTable.setItem(0, 1, QTableWidgetItem("No data"))
                self.statsTable.setItem(0, 2, QTableWidgetItem("No data"))

    def populate_business_table(self, table, business_data, headers):
        table.setRowCount(0)
        table.setColumnCount(len(headers))
        table.setHorizontalHeaderLabels(headers)
        for name, score in business_data:
            row_position = table.rowCount()
            table.insertRow(row_position)
            table.setItem(row_position, 0, QTableWidgetItem(name))
            table.setItem(row_position, 1, QTableWidgetItem(f"{score:.2f}"))  

  
    
    def update_popular_businesses(self, zipcode, category):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT businesses.name, businesses.stars, businesses.review_count, businesses."numCheckins"
                FROM businesses
                WHERE businesses.postal_code = %s AND businesses.categories LIKE %s
                ORDER BY businesses.review_count DESC, businesses."numCheckins" DESC
                LIMIT 10;
            """, (zipcode, '%' + category + '%',))
            businesses = cur.fetchall()


            popular_businesses = [
                (
                    business[0], 
                    business[1], 
                    business[2],  
                    business[3], 
                    calculate_popularity_score(business[3], business[2])  
                )
                for business in businesses
            ]

            self.popularBusinessTable.setRowCount(0)
            for business in popular_businesses:
                row_pos = self.popularBusinessTable.rowCount()
                self.popularBusinessTable.insertRow(row_pos)
                for i, item in enumerate(business):
                    self.popularBusinessTable.setItem(row_pos, i, QTableWidgetItem(str(item)))


    

    def update_successful_businesses(self, zipcode, category):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT businesses.name, businesses.review_count, businesses."numCheckins", MAX(reviews.date) as last_review_date, AVG(reviews.stars) as avg_rating
                FROM businesses
                JOIN reviews ON reviews.business_id = businesses.business_id
                WHERE businesses.postal_code = %s AND businesses.categories LIKE %s
                GROUP BY businesses.name, businesses."numCheckins", businesses.review_count
                ORDER BY businesses.review_count DESC, businesses."numCheckins" DESC
                LIMIT 10;
            """, (zipcode, '%' + category + '%',))
            businesses = cur.fetchall()

            successful_businesses = [
                (
                    business[0],  
                    business[1],  
                    business[2], 
                    calculate_success_score(business[3], business[4], business[2])  # Success Score calculated in Python
                )
                for business in businesses
            ]

            self.successfulBusinessTable.setRowCount(0)
            for business in successful_businesses:
                row_pos = self.successfulBusinessTable.rowCount()
                self.successfulBusinessTable.insertRow(row_pos)
                self.successfulBusinessTable.setItem(row_pos, 0, QTableWidgetItem(str(business[0])))  
                self.successfulBusinessTable.setItem(row_pos, 1, QTableWidgetItem(str(business[1])))  
                self.successfulBusinessTable.setItem(row_pos, 2, QTableWidgetItem(str(business[2])))  
                self.successfulBusinessTable.setItem(row_pos, 3, QTableWidgetItem(f"{business[3]:.2f}"))  

    def update_top_categories(self, zipcode):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT category, COUNT(*) FROM (
                    SELECT UNNEST(string_to_array(categories, ', ')) AS category 
                    FROM businesses WHERE postal_code = %s
                ) AS cat GROUP BY category ORDER BY COUNT(*) DESC;
            """, (zipcode,))
            categories = cur.fetchall()
            self.categoriesTable.setRowCount(len(categories))
            for i, (category, count) in enumerate(categories):
                self.categoriesTable.setItem(i, 0, QTableWidgetItem(category))
                self.categoriesTable.setItem(i, 1, QTableWidgetItem(str(count)))


if __name__ == '__main__':
    app = QApplication([])
    ex = MyApp()
    ex.show()
    app.exec_()