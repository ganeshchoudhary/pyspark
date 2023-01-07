from reader.cassendraSession import create_cassandra_cluster_session
import logging


def create_schema(filename):
    with open(filename, mode='r') as f:
        logger = logging.getLogger(__name__)
        cassandra = create_cassandra_cluster_session()
        txt = f.read()
        stmts = txt.split(r';')
        for i in stmts:
            stmt = i.strip()
            if stmt != '':
                print('Executing "' + stmt + '"')
                cassandra.execute(stmt)




def initial_schema():
    create_schema("turncatDb.sql")
    create_schema("sonata_customer_actual_transactions_table.sql")
    create_schema("sonata_customer_event_logging_table.sql")
    create_schema("sonata_customer_recent_transaction_detail.sql")
    create_schema("sonata_customer_rejected_transactions_table.sql")
    create_schema("sonata_customer_version_tracker_table.sql")
    create_schema("sonata_inactive_account_capture.sql")
    create_schema("sonata_incremental_account_capture.sql")

if __name__ == '__main__':

    initial_schema()
