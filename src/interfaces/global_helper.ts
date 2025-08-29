//need  to confirm keep this or remove
export interface blocks_trnx {
    block_number: number,
    tx_id: string | '',
    value: string | '',
    fromAddress: string | '',
    toAddress: string | '',
    retry_count: number,
    key: string | null,
    pair: string | null
}

export interface blocksTrnx {
    blockNumber: number,
    txId: string | '',
    value: string | '',
    fromAddress: string | '',
    toAddress: string | '',
    gasUsed: number | null,
    gasPrice: number | null
}

export interface pending_trnx {
    tx_id: string;
    coin_id: number,
    is_token: boolean,
    token_type: string | null,
    token_address: string
}