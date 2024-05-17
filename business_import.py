import psycopg2
import datetime

def connect_db():
    return psycopg2.connect(
        dbname="milestone1db",
        user="postgres",
        password="admin",
        host="localhost"
    )

def fetch_business_metrics(cursor):
    cursor.execute("""
        SELECT 
            b.business_id,
            COUNT(DISTINCT r.user_id) AS review_count,
            COUNT(DISTINCT ci.user_id) AS numCheckins,
            AVG(r.stars) AS avg_rating,
            MAX(r.date) AS last_review_date
        FROM 
            Businesses b
        LEFT JOIN Reviews r ON r.business_id = b.business_id
        LEFT JOIN CheckIns ci ON ci.business_id = b.business_id
        GROUP BY b.business_id
    """)
    return cursor.fetchall()

def calculate_popularity_score(numCheckins, review_count):
    checkin_weight = 0.5
    review_weight = 0.5

    popularity_score = (checkin_weight * numCheckins) + \
                       (review_weight * review_count)

    return popularity_score

def calculate_success_score(last_review_date, avg_rating, numCheckins):
    checkin_weight = 0.4
    review_weight = 0.2
    avg_rating = .4
    #business_age = (datetime.datetime.now() - last_review_date).days / 365
    
    #normalize avg_rating by dividing by 5
    normalized_review_rating = avg_rating / 5
    
    #num checkins shouldnt devide by zero
    success_score = (checkin_weight * (numCheckins / float(max(numCheckins, 1)))) + \
                    (review_weight * normalized_review_rating)
    return success_score

def update_businesses(cursor, business_metrics):
    for metrics in business_metrics:
        business_id, review_count, numCheckins, avg_rating, last_review_date = metrics
        popularity_score = calculate_popularity_score(numCheckins, review_count, avg_rating)
        success_score = calculate_success_score(last_review_date, avg_rating, numCheckins, review_count)
        cursor.execute("""
            UPDATE Businesses
            SET 
                popularity_score = %s,
                success_score = %s
            WHERE business_id = %s;
        """, (review_count, numCheckins, avg_rating, popularity_score, success_score, business_id))
    cursor.connection.commit()

def main():
    conn = connect_db()
    cursor = conn.cursor()
    
    business_metrics = fetch_business_metrics(cursor)
    
    update_businesses(cursor, business_metrics)
    
    cursor.close()
    conn.close()

if __name__ == '__main__':
    main()

