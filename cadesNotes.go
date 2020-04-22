package main

//Introduction:
/* Rory's Code */


// func splitDatabase(source, outputPattern string, m int) ([]string, error) {
//     mainDB, err := openDatabase(source)
//     var allSplitDBs []*sql.DB
//     var outputDBs []string

//     for i := 0; i < m; i++ {
//         //substituting i for %d
//         s := fmt.Sprintf(outputPattern, i)
//         var splitDB *sql.DB
//         splitDB, err = createDatabase(s)
//         allSplitDBs = append(allSplitDBs, splitDB)
//         outputDBs = append(outputDBs, s)
//     }

//     selectStatement := `SELECT key, value FROM pairs;`
//     rows, err := mainDB.Query(selectStatement)

//     keysProcessed := 0
//     i := 0
//     for rows.Next() {
//         // fmt.Println(keysProcessed)
//         // read the data using rows.Scan
//         var key string
//         var value string
//         err := rows.Scan(&key, &value)
//         // process the result
//         db := allSplitDBs[i]
//         _, err = db.Exec(`insert into pairs (key, value) values (?, ?)`, key, value)
//         // check for error, etc.
//         if err != nil {
//             fmt.Println(err)
//         }
//         i++
//         keysProcessed++
//         if i >= m {
//             i = 0
//         }
//     }
//     if err := rows.Err(); err != nil {
//         // handle the error
//         fmt.Println("we have an error at splitDatabase rows.Err()")
//         fmt.Println(err)
//     }
//     //close allSplitDBs and input DB
//     //this can be done with defer
//     mainDB.Close()

//     for i := 0; i < m; i++ {
//         allSplitDBs[i].Close()
//     }

//     //if ever err:
//     //return err
//     return outputDBs, err
// }