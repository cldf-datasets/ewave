
def test_valid(cldf_dataset, cldf_logger):
    assert cldf_dataset.validate(log=cldf_logger)


def test_extent(cldf_sqlite_database, cldf_logger):
    assert cldf_sqlite_database.query('select count(*) from languagetable')[0][0] == 77
    assert cldf_sqlite_database.query('select count(*) from parametertable')[0][0] == 235
