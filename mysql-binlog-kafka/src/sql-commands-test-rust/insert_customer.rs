use mysql::prelude::*;
use mysql::*;

#[derive(Debug, PartialEq, Eq)]
struct Payment {
    customer_id: i32,
    amount: i32,
    account_name: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
struct Customer {
    id: i32,
    account_name: Option<String>,
}
fn main() -> std::result::Result<(), Error> {
    let url = if let Ok(url) = std::env::var("DATABASE_URL") {
        let opts = Opts::from_url(&url).expect("DATABASE_URL invalid");
        if opts
            .get_db_name()
            .expect("a database name is required")
            .is_empty()
        {
            panic!("database name is empty");
        }
        url
    } else {
        "mysql://root:password@127.0.0.1:3307/mysql".to_string()
    };
    let pool = Pool::new(url.as_str())?;
    let mut conn = pool.get_conn()?;

    // Let's create a table for payments.
    conn.query_drop(
        r"CREATE TEMPORARY TABLE payment (
        customer_id int not null,
        amount int not null,
        account_name text
    )",
    )?;

        // Let's create a table for payments.
        conn.query_drop(
            r"CREATE TEMPORARY TABLE customer (
            id int not null,
            account_name text
        )",
        )?;
    
    let customers = vec![
        Customer{
            id: 1,
            account_name: None,
        },
        Customer{
            id: 2,
            account_name: None,
        },
        Customer{
            id: 3,
            account_name: Some("czy".into()),
        }
    ];

    let payments = vec![
        Payment {
            customer_id: 1,
            amount: 2,
            account_name: None,
        },
        Payment {
            customer_id: 3,
            amount: 4,
            account_name: Some("foo".into()),
        },
        Payment {
            customer_id: 5,
            amount: 6,
            account_name: None,
        },
        Payment {
            customer_id: 7,
            amount: 8,
            account_name: None,
        },
        Payment {
            customer_id: 9,
            amount: 10,
            account_name: Some("bar".into()),
        },
    ];

    // Now let's insert payments to the database
    conn.exec_batch(
        r"INSERT INTO payment (customer_id, amount, account_name)
      VALUES (:customer_id, :amount, :account_name)",
        payments.iter().map(|p| {
            params! {
                "customer_id" => p.customer_id,
                "amount" => p.amount,
                "account_name" => &p.account_name,
            }
        }),
    )?;

    conn.exec_batch(
        r"INSERT INTO customer (id, account_name)
      VALUES (:id, :account_name)",
        customers.iter().map(|c| {
            params! {
                "id" => c.id,
                "account_name" => &c.account_name,
            }
        }),
    )?;

    // Let's select payments from database. Type inference should do the trick here.
    let selected_payments = conn.query_map(
        "SELECT customer_id, amount, account_name from payment",
        |(customer_id, amount, account_name)| Payment {
            customer_id,
            amount,
            account_name,
        },
    )?;

    let selected_customers = conn.query_map(
        "SELECT id, account_name from customer",
        |(id, account_name)| Customer {
            id,
            account_name,
        },
    )?;

    // Let's make sure, that `payments` equals to `selected_payments`.
    // Mysql gives no guaranties on order of returned rows
    // without `ORDER BY`, so assume we are lucky.
    assert_eq!(payments, selected_payments);
    dbg!(selected_payments);
    assert_eq!(customers, selected_customers);
    dbg!(selected_payments);
    println!("Yay!");

    Ok(())
}
