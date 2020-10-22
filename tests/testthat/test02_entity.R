context("Table entities")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Authentication tests skipped: ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_STORAGE_RG")
storname <- Sys.getenv("AZ_TEST_STORAGE_HNS")

if(rgname == "" || storname == "")
    skip("Table client tests skipped: resource names not set")

sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)
stor <- sub$get_resource_group(rgname)$get_storage_account(storname)
options(azure_storage_progress_bar=FALSE)

endp <- stor$get_table_endpoint()
tab <- create_storage_table(endp, make_name())

test_that("Entity methods work",
{
    etag <- insert_table_entity(tab, list(RowKey="row1", PartitionKey="part1", x=1, y=2))
    expect_type(etag, "character")

    etag2 <- update_table_entity(tab, '{"RowKey":"row1", "PartitionKey":"part1", "z":3}', etag=etag)
    expect_type(etag2, "character")

    expect_error(update_table_entity(tab, list(RowKey="row1", PartitionKey="part1", w=4), etag=etag))

    expect_silent(update_table_entity(tab, list(RowKey="row1", PartitionKey="part1", w=4)))

    expect_is(get_table_entity(tab, "row1", "part1"), "list")

    expect_is(list_table_entities(tab), "data.frame")
    expect_is(list_table_entities(tab, as_data_frame=FALSE), "list")

    expect_silent(delete_table_entity(tab, row_key="row1", partition_key="part1"))

    data <- do.call(rbind, replicate(10, iris, simplify=FALSE))
    names(data) <- sub("\\.", "_", names(data))
    expect_silent(import_table_entities(tab, data,
        row_key=row.names(data),
        partition_key=data$Species))

    lst <- list_table_entities(tab)
    expect_true(is.data.frame(lst) && nrow(lst) == nrow(data))

    lst2 <- list_table_entities(tab, filter="Species eq 'setosa'", select="Sepal_Length,Sepal_Width")
    expect_true(is.data.frame(lst2) && nrow(lst2) == nrow(data)/3 && ncol(lst2) == 2)
})


teardown({
    lst <- list_storage_tables(endp)
    lapply(lst, delete_storage_table, confirm=FALSE)
})
