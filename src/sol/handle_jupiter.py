from common.make_tx import make_swap_tx
from sol.handle_simple import handle_unknown_detect_transfers


def handle_jupiter_aggregator_v2(exporter, txinfo):
    txinfo.comment = "jupiter_aggregator"

    # Problem : transfers_net contains the empty transfers_out
    transfers_in, transfers_out, _ = txinfo.transfers_net
    print ("transfers_in ", transfers_in)
    print ("transfers_out ", transfers_out)

    if len(transfers_in) == 1 and len(transfers_out) == 1:
        sent_amount, sent_currency, _, _ = transfers_out[0]
        received_amount, received_currency, _, _ = transfers_in[0]
        row = make_swap_tx(txinfo, sent_amount, sent_currency, received_amount, received_currency)
        exporter.ingest_row(row)
    else:
        handle_unknown_detect_transfers(exporter, txinfo)

def handle_jupiter_aggregator_v2_new (exporter, txinfo):
    txinfo.comment = "jupiter_aggregator"

    # Problem : transfers_net contains the empty transfers_out
    transfers_in, transfers_out, _ = txinfo.transfers_net
    print ("transfers_in ", transfers_in)
    print ("transfers_out ", transfers_out)

    if len(transfers_in) == 1 and len(transfers_out) == 1:
        sent_amount, sent_currency, _, _ = transfers_out[0]
        received_amount, received_currency, _, _ = transfers_in[0]
        row = make_swap_tx(txinfo, sent_amount, sent_currency, received_amount, received_currency)
        exporter.ingest_row(row)
        print ("TRYING TO EXPORT_PRINT")
        #exporter.export_print()
        exporter.export_string_new()
     #else if len(transfers_in) == 1 and len(transfers_out) == 0:
     #   sent_amount, sent_currency, _, _ = transfers_out[0]
     #   sent_amount, sent_currency, _, _ = transfers_in[0]
     #   received_amount, received_currency, _, _ = transfers_in[0]
     #   row = make_swap_tx(txinfo, sent_amount, sent_currency, received_amount, received_currency)
     #   exporter.ingest_row(row)
    else:
        handle_unknown_detect_transfers(exporter, txinfo)
