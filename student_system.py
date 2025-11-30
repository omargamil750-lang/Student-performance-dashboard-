def main():
    database = "student_performance.db"
    csv_file = "student_performance.csv"
    
    # Debug: Print current directory and files
    print(f"Current directory: {os.getcwd()}")
    print(f"Files in directory: {os.listdir()}")
    
    # Check if CSV file exists
    if not os.path.exists(csv_file):
        print(f"Error: {csv_file} not found in {os.getcwd()}")
        return
        
    print(f"Found {csv_file}, checking contents...")
    
    # Create a database connection
    conn = create_connection(database)
    if conn is not None:
        try:
            # Create tables
            create_tables(conn)
            
            # Import data
            import_student_data(conn, csv_file)
            
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            # Close connection
            conn.close()
            print("Database connection closed")
    else:
        print("Error! Cannot create the database connection.")

if __name__ == '__main__':
    main()