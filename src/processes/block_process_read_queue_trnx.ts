import { config } from "../config/config";
// import rabbitMq from "../config/rabbitMq";
import { Blockchain_Helper, Utility_Helper } from "../helpers";
import { blocks_trnx } from "../interfaces/global_helper";
import { TransactionReceipt } from "web3-core";
import bscProcessHelper from "./process.helper";
import { TxType } from "../types/ERC20Tokens.types";
import { TrnxTypeEnum } from "../enum";
import { global_helper } from "../helpers/global_helper";
import kafkaHelper from "../config/kafka.helper";

class queue_transactions {
    public approval: string | null;
    public swap: string | null;
    public transfer: string | null;

    public bsc_block_tx_topic = config.KAFKA_BLOCKS.BSC_BLOCK_TX_TOPIC;
    public bsc_block_tx_grpup_id = config.KAFKA_BLOCKS.BSC_BLOCK_TX_GROUP_ID;
    public bsc_behind_block_tx_topic = config.KAFKA_BLOCKS.BSC_BEHIND_BLOCK_TX_TOPIC;
    public bsc_behind_block_tx_group_id = config.KAFKA_BLOCKS.BSC_BEHIND_BLOCK_TX_GROUP_ID;
    public bsc_deposit_withdraw_topic = config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_TOPIC;

    constructor() {
        this.approval = Blockchain_Helper.BSC_Web3.utils.sha3('Approval(address,address,uint256)');
        this.swap = Blockchain_Helper.BSC_Web3.utils.sha3('Swap(address,uint256,uint256,uint256,uint256,address)');
        this.transfer = Blockchain_Helper.BSC_Web3.utils.sha3('Transfer(address,address,uint256)');

    }

    public startBlockTxQueue = async () => {
        let consumer = await kafkaHelper.kafka.consumer({ groupId: this.bsc_block_tx_grpup_id });
        await consumer.subscribe({ topic: this.bsc_block_tx_topic, fromBeginning: true });

        await consumer.run({
            eachMessage: ({ topic, partition, message }: { topic: any, partition: any, message: any }) => {
                // console.log("🚀 ~ startBlockTxQueue topic", topic);
                // console.log("🚀 ~ startBlockTxQueue message.value", message.value.toString());
                this.read_trnx(JSON.parse(message.value.toString()))
            }
        }).catch((err: Error) => {
            console.log("🔥 ~ startBlockTxQueue consumer error", err);
        });
    };

    public startBehindBlockTxQueue = async () => {
        let consumer = await kafkaHelper.kafka.consumer({ groupId: this.bsc_behind_block_tx_group_id });
        await consumer.subscribe({ topic: this.bsc_behind_block_tx_topic, fromBeginning: true });

        await consumer.run({
            eachMessage: ({ topic, partition, message }: { topic: any, partition: any, message: any }) => {
                // console.log("🚀 ~ startBehindBlockTxQueue topic", topic);
                // console.log("🚀 ~ startBehindBlockTxQueue message.value", message.value.toString());
                this.read_trnx(JSON.parse(message.value.toString()))
            }
        }).catch((err: Error) => {
            console.log("🔥 ~ startBehindBlockTxQueue consumer error", err);
        });
    };

    // public consume_queue_trnx = async () => {

    //     // Latest transactions
    //     rabbitMq.only_assert_queue(config.TRANSACTIONS.bsc_all_blocks);
    //     console.error(`RABBITMQ_BLOCK_ALL_TRNX_PROCESS >>>`, config.TRANSACTIONS.bsc_all_blocks);
    //     // rabbitMq.only_consume_queue(config.TRANSACTIONS.bsc_all_blocks, this.read_trnx);
    //     for (let i = 1; i <= Number(config.TRANSACTIONS.bsc_transaction_consumer_count); i++) {
    //         rabbitMq.only_consume_queue(config.TRANSACTIONS.bsc_all_blocks, this.read_trnx);
    //         // RabbitMq.consumeQueue(this.RABBITMQ_BLOCK_ALL_TRNX_PROCESS, this.GetTxDetail);
    //     }
    //     // Behined transactions
    //     rabbitMq.only_assert_queue(config.TRANSACTIONS.bsc_behined_transactions);
    //     console.error(`RABBITMQ_BLOCK_ALL_TRNX_PROCESS >>>`, config.TRANSACTIONS.bsc_behined_transactions);
    //     rabbitMq.only_consume_queue(config.TRANSACTIONS.bsc_behined_transactions, this.read_trnx);
    //     //==========================================================
    // }
    public read_trnx = async (data: blocks_trnx) => {
        try {
            // console.log('consume_queue_trnx >>>>', data);
            const { getTransactionReceipt } = Blockchain_Helper.BSC_Web3.eth;
            const txReceipt = await getTransactionReceipt(data.tx_id);
            if (txReceipt == null) {
                if (Number(data.retry_count) < 6) {
                    data.retry_count = Number(data.retry_count) + 1;
                    // rabbitMq.send_tx_to_queue(config.TRANSACTIONS.bsc_behined_transactions, Buffer.from(JSON.stringify(data)))
                    await kafkaHelper.produceMessage(this.bsc_behind_block_tx_topic, data)
                }
            }
            // console.log('txReceipt >>>', txReceipt)
            if (txReceipt?.status === false && txReceipt !== null) {
                console.log("(txReceipt.status === false && txReceipt !== null")
            } else {
                await bsc_tx_status_deposit_withdraw.buildTx(txReceipt, data)
            }
        } catch (error: any) {
            console.error("Error in read_trnx>>>", error)
            if (Number(data.retry_count) < 6) {
                data.retry_count = Number(data.retry_count) + 1;
                console.log("Adding transaction in queue>>>", data.retry_count, "transaction id>>>", data)
                // rabbitMq.send_tx_to_queue(config.TRANSACTIONS.bsc_behined_transactions, Buffer.from(JSON.stringify(data)))
                await kafkaHelper.produceMessage(this.bsc_behind_block_tx_topic, data)
            }
        }
    }
    public buildTx = async (txReceipt: TransactionReceipt, data: blocks_trnx) => {
        try {
            if (txReceipt.status == true && txReceipt.logs.length > 0) {
                const gasUsed = txReceipt.gasUsed ? txReceipt.gasUsed : 0;
                const gasInBsc = await Blockchain_Helper.convertWeiToBsc(
                    await Utility_Helper.bigNumberSafeMath(gasUsed, '*', txReceipt.effectiveGasPrice)
                );
                for await (const log of txReceipt.logs) {
                    const isToken: any = await bscProcessHelper.checkIfContract(log.address);
                    if (isToken !== null && log.topics[2] !== undefined) {
                        if (log?.topics && log?.topics[0] == this.approval) {
                            continue;
                        } else if (log?.topics && log?.topics[0] == this.transfer) {
                            const decimalPrecision = isToken.decimals.toString().length - 1;
                            let finalAmount = await Utility_Helper.bigNumberSafeMath(log.data, '/', Math.pow(10, decimalPrecision))
                            finalAmount = await Utility_Helper.exponentialToDecimal(finalAmount)
                            let toAddress: string = '0x' + log.topics[2].substring(26, log.topics[2].length);
                            /* in case of swap trnx */
                            let fromAddress: string = '0x' + log.topics[1].substring(26, log.topics[1].length);
                            const txData: TxType = {
                                txId: log.transactionHash,
                                fromAddress: fromAddress,
                                toAddress: toAddress,
                                token: isToken,
                                amount: finalAmount,
                                blockId: log.blockNumber,
                                isNFTToken: false,
                                txType: null,
                                trnx_fee: Number(gasInBsc)
                            };
                            await this.checkAddressInDB(txData);
                        }
                    }
                    /** swap transaction then also check bsc */
                    if (log?.topics && log?.topics[0] == this.swap) {
                        await bsc_tx_status_deposit_withdraw.check_bsc_deposit(data, txReceipt);
                    } else if (isToken == null && log.topics[2] !== undefined && log?.topics[0] == this.transfer) {
                        await bsc_tx_status_deposit_withdraw.check_bsc_deposit(data, txReceipt)
                    }
                }
            } else {
                await bsc_tx_status_deposit_withdraw.check_bsc_deposit(data, txReceipt);
            }
        } catch (error: any) {
            console.error("buildTx error >>>", (error as Error).message);
            await global_helper.save_error_logs('Bsc_buildTx', error.message)
        }
    };
    public check_bsc_deposit = async (data: blocks_trnx, txReceipt: any) => {
        try {
            const gasUsed = txReceipt.gasUsed ? txReceipt.gasUsed : 0;
            const gasInbsc = await Blockchain_Helper.convertWeiToBsc(
                await Utility_Helper.bigNumberSafeMath(
                    gasUsed, '*', txReceipt.effectiveGasPrice
                )
            );
            // const { getTransaction } = Blockchain_Helper.Web3.eth;
            // const transaction = await getTransaction(txId);

            /** for check the bsc deposit */
            if (
                txReceipt.status == true,
                data.value !== '0' &&
                data.toAddress &&
                data.block_number
            ) {
                let amount = parseFloat(Blockchain_Helper.BSC_Web3.utils.fromWei(data.value, 'ether'));
                const txData: TxType = {
                    txId: data.tx_id,
                    fromAddress: data.fromAddress.toLowerCase(),
                    toAddress: data.toAddress.toLowerCase(),
                    token: null,
                    amount,
                    blockId: data?.block_number,
                    isNFTToken: false,
                    txType: TrnxTypeEnum.NULL,
                    trnx_fee: Number(gasInbsc)
                };
                await this.checkAddressInDB(txData);
            }
        } catch (error: any) {
            console.error("💥 ~ ~ check_bsc_deposit error", (error as Error).message);
            await global_helper.save_error_logs('BSC_check_deposit', error.message)

        }
    }
    public checkAddressInDB = async (txData: TxType) => {
        try {
            let ourTrnx: any = null;
            let withdraw = await bscProcessHelper.check_our_wallet_address(txData.fromAddress);
            if (withdraw) {
                console.log("Found the user in FROM (WITHDRAW) tx 😸");
                txData.txType = TrnxTypeEnum.WITHDRAW;
                ourTrnx = txData;
            }
            let deposit = await bscProcessHelper.check_our_wallet_address(txData.toAddress);
            if (deposit) {
                console.log("Found the user in to (DEPOSIT) tx 😸");
                txData.txType = TrnxTypeEnum.DEPOSIT;
                ourTrnx = txData;
            }
            if (ourTrnx) {
                await bsc_tx_status_deposit_withdraw.addTxToQueue(
                    ourTrnx,
                    this.bsc_deposit_withdraw_topic || ""
                );
            }
        } catch (err: any) {
            console.error("Error in checkAddressInDB>>", err)
        }
    }
    public addTxToQueue = async (data: TxType, queueName: string) => {
        try {
            await kafkaHelper.produceMessage(queueName, data);
            // rabbitMq.send_tx_to_queue(queueName, Buffer.from(JSON.stringify(data)))
            // await rabbitMq.assertQueue(queueName);
            // await rabbitMq.sendToQueue(queueName, Buffer.from(JSON.stringify(data)));
        } catch (err: any) {
            console.error("addTxToQueue", (err as Error).message);
            await global_helper.save_error_logs('ETH_checkAddressInDB', err.message)

        }
    };

}
export const bsc_tx_status_deposit_withdraw = new queue_transactions()
