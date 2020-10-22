context("Cosmos DB table endpoint")

cosmosdb <- Sys.getenv("AZ_TEST_COSMOSDB_TABLE")
key <- Sys.getenv("AZ_TEST_COSMOSDB_TABLE_KEY")

if(cosmosdb == "" || key == "")
    skip("Cosmos DB table client tests skipped: resource names not set")

endp <- table_endpoint(sprintf("https://%s.table.cosmos.azure.com:443", cosmosdb), key=key)

test_that("Table endpoint works",
{
    expect_true(is_empty(list_storage_tables(endp)))

    # ways of creating a container
    name1 <- make_name()
    tab <- storage_table(endp, name1)
    create_storage_table(tab)
    create_storage_table(endp, make_name())

    lst <- list_storage_tables(endp)
    expect_true(is.list(lst) && inherits(lst[[1]], "storage_table") && length(lst) == 2)

    expect_identical(tab$name, lst[[name1]]$name)

    expect_silent(delete_storage_table(tab, confirm=FALSE))
})


teardown({
    lst <- list_storage_tables(endp)
    lapply(lst, delete_storage_table, confirm=FALSE)
})
