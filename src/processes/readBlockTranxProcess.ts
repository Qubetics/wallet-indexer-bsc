import { Blockchain_Helper } from "../helpers/index";
import { config } from "../config/config";
import bscProcessHelper from "./process.helper";
import { KEYS } from "../enum";
import { blocks_trnx } from "../interfaces/global_helper";
import { BscOldBLockModel } from "../models";
import { global_helper } from "../helpers/global_helper";
import kafkaHelper from "../config/kafka.helper";
import { ethers } from "ethers";


class ReadBlockTrnxProcess {
    public minBlockConfirmation: number;
    public swap: string | null;
    public approval: string | null;

    public bsc_block_topic = config.KAFKA_BLOCKS.BSC_BLOCK_TOPIC;
    public bsc_block_group = config.KAFKA_BLOCKS.BSC_BLOCK_GROUP_ID;
    public bsc_behind_block_topic = config.KAFKA_BLOCKS.BSC_BEHIND_BLOCK_TOPIC;
    public bsc_behind_block_group = config.KAFKA_BLOCKS.BSC_BEHIND_BLOCK_GROUP_ID;

    public bsc_block_tx_topic = config.KAFKA_BLOCKS.BSC_BLOCK_TX_TOPIC;
    public bsc_behind_block_tx_topic = config.KAFKA_BLOCKS.BSC_BEHIND_BLOCK_TX_TOPIC;

    constructor() {
        this.minBlockConfirmation = Number(config.MIN_BLOCK_CONFORMATIONS);
        this.swap = ethers.id('Swap(address,uint256,uint256,uint256,uint256,address)');
        this.approval = ethers.id('Approval(address,address,uint256)');
    }

    public startLatestBlockEvenConsumer = async () => {
        const consumer = await kafkaHelper.kafka.consumer({
            groupId: `${this.bsc_block_group}_even`,
        });
        await consumer.subscribe({
            topic: `${this.bsc_block_topic}_even`,
            fromBeginning: true,
        });
        await consumer.run({
            eachMessage: async ({
                topic,
                partition,
                message,
            }: {
                topic: any;
                partition: any;
                message: any;
            }) => {
                console.log('🚀 ~ startLatestBlockEvenConsumer topic', topic);
                console.log('🚀 ~ startLatestBlockEvenConsumer message.value', message.value.toString());
                const blockId = JSON.parse(message.value.toString());
                this.getTxFromBlock(blockId, KEYS.latestBlock);
            },
        }).catch((err: Error) => {
            console.log('🔥 ~ startDepositWithdrawQueue consumer error', err);
        });
    };

    public startLatestBlockOddConsumer = async () => {
        const consumer = await kafkaHelper.kafka.consumer({
            groupId: `${this.bsc_block_group}_odd`,
        });
        await consumer.subscribe({
            topic: `${this.bsc_block_topic}_odd`,
            fromBeginning: true,
        });
        await consumer.run({
            eachMessage: async ({
                topic,
                partition,
                message,
            }: {
                topic: any;
                partition: any;
                message: any;
            }) => {
                console.log('🚀 ~ startLatestBlockOddConsumer topic', topic);
                console.log('🚀 ~ startLatestBlockOddConsumer message.value', message.value.toString());
                const blockId = JSON.parse(message.value.toString());
                this.getTxFromBlock(blockId, KEYS.latestBlock);
            },
        }).catch((err: Error) => {
            console.log('🔥 ~ startDepositWithdrawQueue consumer error', err);
        });
    };

    public startBehindBlockConsumer = async () => {
        const consumer = await kafkaHelper.kafka.consumer({
            groupId: this.bsc_behind_block_group,
        });
        await consumer.subscribe({
            topic: this.bsc_behind_block_topic,
            fromBeginning: true,
        });
        await consumer.run({
            eachMessage: async ({
                topic,
                partition,
                message,
            }: {
                topic: any;
                partition: any;
                message: any;
            }) => {
                console.log('🚀 ~ startBehindBlockConsumer topic', topic);
                console.log('🚀 ~ startBehindBlockConsumer message.value', message.value.toString());
                const blockId = JSON.parse(message.value.toString());
                this.getTxFromBlock(blockId, KEYS.behinedBlock);
            },
        }).catch((err: Error) => {
            console.log('🔥 ~ startDepositWithdrawQueue consumer error', err);
        });
    };

    // public async getBlocks() {
    //     try {
    //         let old_block: any = await bscProcessHelper.getKeyValuePair(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.OLD_BLOCK_BSC)
    //         let new_block: any = await bscProcessHelper.getKeyValuePair(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.NEW_BLOCK_BSC)
    //         if (!old_block) {
    //             console.log("No block present in old block")
    //             await bscProcessHelper.setBlocks(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.OLD_BLOCK_BSC, this.minBlockConfirmation, null)
    //         }
    //         if (!new_block) {
    //             console.log("No block present in new block")
    //             await bscProcessHelper.setBlocks(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.NEW_BLOCK_BSC, this.minBlockConfirmation, null)
    //         }
    //         if (old_block && new_block) {
    //             let oldBlock: number = Number(old_block);
    //             let newBlock = Number(new_block);
    //             if (newBlock <= oldBlock) {
    //                 await bscProcessHelper.setBlocks(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.NEW_BLOCK_BSC, this.minBlockConfirmation, newBlock)
    //             } else {
    //                 let block_diff: number = newBlock - oldBlock;
    //                 if (block_diff > 50) {
    //                     await bscProcessHelper.diff_blocks_add_in_db(newBlock, oldBlock)
    //                     await bscProcessHelper.setKeyValuePair(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.OLD_BLOCK_BSC, newBlock.toString())
    //                     await bscProcessHelper.setBlocks(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.NEW_BLOCK_BSC, this.minBlockConfirmation, newBlock)
    //                 } else {
    //                     await this.readBlocks(oldBlock, newBlock)
    //                 }
    //             }
    //         } else {
    //             console.log(" no old block and no new block")
    //             await global_helper.save_error_logs('Bsc_setBlocks', 'No old block no new block')
    //         }
    //     } catch (err: any) {
    //         console.error("Error in getBlocks", err)
    //         await global_helper.save_error_logs('bsc_getBlocks', err.message)
    //     }
    // }
    // public async readBlocks(blockNumber: number, toBlockNumber: number) {
    //     try {
    //         console.log("blockNumber", blockNumber)
    //         let block_updated: number = blockNumber + 1;
    //         await bscProcessHelper.setKeyValuePair(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.OLD_BLOCK_BSC, block_updated.toString());
    //         console.table({
    //             LATEST_BLOCK: blockNumber,
    //             BLOCK_DIFFERENCE: toBlockNumber - blockNumber,
    //             TO_BLOCK_NUMBER: toBlockNumber
    //         });
    //         this.getTxFromBlock(blockNumber, KEYS.latestBlock);
    //     } catch (err: any) {
    //         console.error("Error in readBlocks>>.", (err as Error).message)
    //         await global_helper.save_error_logs('BSC_readBlocks', err.message)
    //     }
    // }

    private async getTxFromBlock(blockId: number, key: string) {
        try {
            console.log('blockId >>>>', blockId, 'key >>>>', key);
            const blockInfo = await Blockchain_Helper.provider.getBlock(blockId, true);
            if (!blockInfo) {
                console.log("No block info");
                await BscOldBLockModel.create({ block_number: blockId, coin_family: config.STATIC_COIN_FAMILY.BNB, start_block: blockId, end_block: blockId, status: 0 })
                return;
            }
            const transactions = blockInfo.prefetchedTransactions || [];
            if (transactions.length === 0) {
                console.log("No block info transactions");
                return;
            }

            const transactionPromises = transactions.map(async (tx) => {
                const data: blocks_trnx = {
                    block_number: blockId,
                    tx_id: tx.hash || '',
                    value: tx.value.toString(),
                    fromAddress: tx.from || '',
                    toAddress: tx.to || '',
                    retry_count: 0,
                    key: null,
                    pair: null
                };

                data.key = (key === KEYS.latestBlock) ? 'latest' : 'behined & specific';
                const targetTopic = (key === KEYS.latestBlock) ? this.bsc_block_tx_topic : this.bsc_behind_block_tx_topic;

                try {
                    await kafkaHelper.produceMessage(targetTopic, data);
                } catch (err) {
                    console.error("Error while producing message for transaction", tx.hash, err);
                }
            });
            await Promise.allSettled(transactionPromises);
        } catch (err: any) {
            console.error("Error in getTxFromBlock:", (err as Error).message);
            await global_helper.save_error_logs('bsc_getTxFromBlock', err.message)
        }
    }

    // private async readBehindBlock() {
    //     try {
    //         // REDIS BASED
    //         const data_exist: any = await bscProcessHelper.getKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_BSC)
    //         if (data_exist) {
    //             const data_exists = JSON.parse(data_exist);
    //             if (data_exists) {
    //                 const { id, start_block, end_block } = data_exists;
    //                 if (end_block > start_block) {
    //                     const newStartBlock = start_block + 1;
    //                     const updateKeyData = {
    //                         id: id,
    //                         start_block: newStartBlock,
    //                         end_block: end_block,
    //                         diff: end_block - newStartBlock
    //                     }
    //                     await bscProcessHelper.setKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_BSC, JSON.stringify(updateKeyData))

    //                     console.log("This block go for getTxFromBlock")
    //                     bsc_blocks_process.getTxFromBlock(start_block, KEYS.behinedBlock);

    //                 } else {
    //                     console.log("id>>>>>>>>>>>", id)
    //                     await BscOldBLockModel.update({ status: 1 }, { where: { id: id } })
    //                     await bscProcessHelper.deleteKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_BSC)
    //                     await bscProcessHelper.behindBlocks()

    //                 }
    //             }
    //         } else {
    //             // DB BASED
    //             await bscProcessHelper.behindBlocks()
    //         }
    //     } catch (err: any) {
    //         console.error("Error in readBehindBlock>>", err)
    //         await global_helper.save_error_logs('BSC_readBehindBlock', err.message)
    //     }
    // }

    public async readSpecificBlock() {
        try {
            console.log("Entere into readSpecificBlock>>>")
            let specificBlocks: any = await bscProcessHelper.getKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_BSC)
            let specific_block: any = JSON.parse(specificBlocks);
            if (specific_block && Number(specific_block) > 0) {
                console.table({ SPECIFIC_BLOCK: specific_block })
                await this.getTxFromBlock(specific_block, KEYS.readSpecificBlock)
                await bscProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_BSC, '0')
            } else {
                console.log("Specific block is not present>>>>")
                await bscProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_BSC, '0')
            }
        } catch (error: any) {
            console.error("Error in readSpecificBlock>>>", error)
        }
    }

    // public async testingNode() {
    //     try {
    //         const provider = Blockchain_Helper.provider;

    //         // Get Block Number
    //         const latestBlock = await provider.getBlockNumber();
    //         console.log("Block number>>", latestBlock);

    //         const blockInfo = await provider.getBlock(latestBlock, true);
    //         console.log("blockInfo>>", blockInfo);

    //         if (blockInfo && blockInfo.transactions.length > 0) {
    //             let firstTxHash: string;
    //             const firstTx = blockInfo.transactions[0];

    //             if (typeof firstTx === 'string') {
    //                 firstTxHash = firstTx;
    //             } else {
    //                 // Assert the type to access the hash property safely
    //                 firstTxHash = (firstTx as ethers.TransactionResponse).hash;
    //             }
    //             // Get Block Receipt
    //             const txReceipt = await provider.getTransactionReceipt(firstTxHash);
    //             console.log("txreceipt>>", txReceipt);

    //             // Get more details (not reduced in ethers.js)
    //             const transaction = await provider.getTransaction(firstTxHash);
    //             console.log("transaction>>", transaction);
    //         } else {
    //             console.log("No transactions found in the block");
    //         }
    //     } catch (err: any) {
    //         console.error("Error in testingNode>>", err)
    //     }
    // }
}

export const readBlockTrnxProcess = new ReadBlockTrnxProcess();