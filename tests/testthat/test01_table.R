context("Table endpoint")

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

test_that("Table endpoint works",
{
    endp2 <- table_endpoint(stor$properties$primaryEndpoints$table, key=stor$list_keys()[1])
    expect_is(endp, "table_endpoint")
    expect_identical(endp, endp2)

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
