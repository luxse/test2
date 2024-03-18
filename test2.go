package main

import (
    "database/sql"
    "encoding/json"
    "fmt"
    "log"
    "net/http"

    _ "github.com/lib/pq"
    "github.com/gorilla/mux"
)

const (
    host     = "localhost"
    port     = 5432
    user     = "your_username"
    password = "your_password"
    dbname   = "your_dbname"
)

var db *sql.DB

type User struct {
    ID      int    `json:"id"`
    Name    string `json:"name"`
    Balance int    `json:"balance"`
}

type Quest struct {
    ID   int    `json:"id"`
    Name string `json:"name"`
    Cost int    `json:"cost"`
}

func CreateUser(w http.ResponseWriter, r *http.Request) {
    var user User
    json.NewDecoder(r.Body).Decode(&user)
    
    err := db.QueryRow("INSERT INTO users(name, balance) VALUES($1, $2) RETURNING id", user.Name, user.Balance).Scan(&user.ID)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    w.WriteHeader(http.StatusCreated)
    json.NewEncoder(w).Encode(user)
}

func CreateQuest(w http.ResponseWriter, r *http.Request) {
    var quest Quest
    json.NewDecoder(r.Body).Decode(&quest)
    
    err := db.QueryRow("INSERT INTO quests(name, cost) VALUES($1, $2) RETURNING id", quest.Name, quest.Cost).Scan(&quest.ID)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    w.WriteHeader(http.StatusCreated)
    json.NewEncoder(w).Encode(quest)
}

func CompleteQuest(w http.ResponseWriter, r *http.Request) {
    var userQuest struct {
        UserID  int `json:"user_id"`
        QuestID int `json:"quest_id"`
    }
    json.NewDecoder(r.Body).Decode(&userQuest)
    
    var cost int
    err := db.QueryRow("SELECT cost FROM quests WHERE id = $1", userQuest.QuestID).Scan(&cost)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    var userBalance int
    err = db.QueryRow("SELECT balance FROM users WHERE id = $1", userQuest.UserID).Scan(&userBalance)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    if userBalance < cost {
        http.Error(w, "Insufficient balance", http.StatusBadRequest)
        return
    }

    tx, err := db.Begin()
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    _, err = tx.Exec("UPDATE users SET balance = balance - $1 WHERE id = $2", cost, userQuest.UserID)
    if err != nil {
        tx.Rollback()
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    _, err = tx.Exec("INSERT INTO completed_quests(user_id, quest_id) VALUES($1, $2)", userQuest.UserID, userQuest.QuestID)
    if err != nil {
        tx.Rollback()
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    tx.Commit()
    w.WriteHeader(http.StatusNoContent)
}

func GetUserHistory(w http.ResponseWriter, r *http.Request) {
    params := mux.Vars(r)
    userID := params["id"]
    
    var user User
    err := db.QueryRow("SELECT id, name, balance FROM users WHERE id = $1", userID).Scan(&user.ID, &user.Name, &user.Balance)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    var completedQuests []Quest
    rows, err := db.Query("SELECT q.id, q.name, q.cost FROM completed_quests c JOIN quests q ON c.quest_id = q.id WHERE c.user_id = $1", userID)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    defer rows.Close()

    for rows.Next() {
        var quest Quest
        if err := rows.Scan(&quest.ID, &quest.Name, &quest.Cost); err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }
        completedQuests = append(completedQuests, quest)
    }
    if err := rows.Err(); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    userWithHistory := struct {
        User            `json:"user"`
        CompletedQuests []Quest `json:"completed_quests"`
    }{
        User:            user,
        CompletedQuests: completedQuests,
    }

    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(userWithHistory)
}

func main() {
    connectionString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
        host, port, user, password, dbname)

    var err error
    db, err = sql.Open("postgres", connectionString)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    router := mux.NewRouter()

    router.HandleFunc("/users", CreateUser).Methods("POST")
    router.HandleFunc("/quests", CreateQuest).Methods("POST")
    router.HandleFunc("/complete-quest", CompleteQuest).Methods("POST")
    router.HandleFunc("/users/{id}/history", GetUserHistory).Methods("GET")

    log.Fatal(http.ListenAndServe(":8080", router))
}
