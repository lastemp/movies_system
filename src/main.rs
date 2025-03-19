use actix::prelude::*;
use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sqlx::mysql::MySqlPool;
use sqlx::Column;
use sqlx::Row; // Import the Row trait
use std::collections::HashMap; // Import the Rng trait

// Define a struct for the database actor
pub struct DbExecutor(pub MySqlPool);

// Actor implementation for DbExecutor
impl Actor for DbExecutor {
    type Context = Context<Self>; // Use asynchronous Context
}

// Message for executing a query
#[derive(Message)]
#[rtype(result = "Result<Vec<HashMap<String, String>>, sqlx::Error>")]
pub struct ExecuteQuery(pub String);

// Handler for the ExecuteQuery message
impl Handler<ExecuteQuery> for DbExecutor {
    type Result = ResponseFuture<Result<Vec<HashMap<String, String>>, sqlx::Error>>;

    fn handle(&mut self, msg: ExecuteQuery, _: &mut Self::Context) -> Self::Result {
        let pool = self.0.clone();
        let query = msg.0;

        // Use `Box::pin` to return a future
        Box::pin(async move {
            let rows = sqlx::query(&query).fetch_all(&pool).await?;

            // Convert rows into a serializable format (Vec<HashMap<String, String>>)
            let result = rows
                .iter()
                .map(|row| {
                    let mut map = HashMap::new();
                    for (i, column) in row.columns().iter().enumerate() {
                        let column_name = column.name().to_string();
                        let value: String = row.try_get(i).unwrap_or_default();
                        map.insert(column_name, value);
                    }
                    map
                })
                .collect();

            Ok(result)
        })
    }
}

// Function to create a MySQL connection pool
pub async fn create_db_pool(db_url: &str) -> MySqlPool {
    MySqlPool::connect(db_url)
        .await
        .expect("Failed to create database pool")
}

// Function to start multiple actors sharing the same thread
pub fn start_db_actors(pool: MySqlPool, num_actors: usize) -> Vec<Addr<DbExecutor>> {
    let mut actors = Vec::new();

    for _ in 0..num_actors {
        let actor = DbExecutor(pool.clone()).start();
        actors.push(actor);
    }

    actors
}

// Define a struct for the query request
#[derive(Serialize, Deserialize)]
struct QueryRequest {
    query: String,
}

// Handler for the `/query` endpoint
async fn execute_query(
    db: web::Data<Vec<Addr<DbExecutor>>>,
    query: web::Json<QueryRequest>,
) -> impl Responder {
    // Use the rand crate to generate a random index
    let actor_index = rand::thread_rng().gen_range(0..db.len());
    let actor = &db[actor_index];

    match actor.send(ExecuteQuery(query.query.clone())).await {
        Ok(result) => match result {
            Ok(rows) => HttpResponse::Ok().json(rows),
            Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
        },
        Err(_) => HttpResponse::InternalServerError().body("Actor mailbox error"),
    }
}

// Main function to start the Actix Web server
#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Database URL (replace with your MySQL credentials)
	let db_url = "mysql://user:password@localhost:3306/mydb";

    // Create a database pool
    let pool = create_db_pool(db_url).await;

    // Start multiple actors sharing the same thread
    let db_actors = start_db_actors(pool, 10); // Start 10 actors

    // Start the Actix Web server
    println!("Starting server at http://127.0.0.1:8080");
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(db_actors.clone()))
            .route("/query", web::post().to(execute_query))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
