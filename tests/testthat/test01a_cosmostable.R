context("Cosmos DB table endpoint")

cosmosdb <- Sys.getenv("AZ_TEST_COSMOSDB_TABLE")
key <- Sys.getenv("AZ_TEST_COSMOSDB_TABLE_KEY")

if(cosmosdb == "" || key == "")
    skip("Cosmos DB table client tests skipped: resource names not set")

endp <- table_endpoint(sprintf("https://%s.table.cosmos.azure.com:443", cosmosdb), key=key)

test_that("Table endpoint works",
{
    expect_true(is_empty(list_azure_tables(endp)))

    # ways of creating a container
    name1 <- make_name()
    tab <- azure_table(endp, name1)
    create_azure_table(tab)
    create_azure_table(endp, make_name())

    lst <- list_azure_tables(endp)
    expect_true(is.list(lst) && inherits(lst[[1]], "azure_table") && length(lst) == 2)

    expect_identical(tab$name, lst[[name1]]$name)

    expect_silent(delete_azure_table(tab, confirm=FALSE))
})


teardown({
    lst <- list_azure_tables(endp)
    lapply(lst, delete_azure_table, confirm=FALSE)
})
