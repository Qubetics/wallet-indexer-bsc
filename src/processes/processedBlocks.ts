import { blockChainHelper } from "../helpers/index";
import { config } from "../config/config";
import bscProcessHelper from "./process.helper";
import { KEYS } from "../enum";
import { blocksTrnx } from "../interfaces/globalHelper";
import { bscOldBLockModel } from "../models";
import { globalHelper } from "../helpers/global.helper";
import kafkaHelper from "../config/kafka.helper";
import { ethers } from "ethers";


class BSCBlockProcess {
    public minBlockConfirmation: number;
    public swap: string | null;
    public approval: string | null;

    public bsc_block_tx_topic = config.KAFKA_BLOCKS.BSC_BLOCK_TX_TOPIC;
    public bsc_behind_block_tx_topic = config.KAFKA_BLOCKS.BSC_BEHIND_BLOCK_TX_TOPIC;

    constructor() {
        this.minBlockConfirmation = Number(config.MIN_BLOCK_CONFORMATIONS);
        this.swap = ethers.id('Swap(address,uint256,uint256,uint256,uint256,address)');
        this.approval = ethers.id('Approval(address,address,uint256)');
    }

    public async readCurrentBlocks() {
        try {
            const results = await bscProcessHelper.getMultipleKeyValuePairs(config.REDISKEYS.BSC_BLOCKS, [config.REDISKEYS.OLD_BLOCK_BSC, config.REDISKEYS.NEW_BLOCK_BSC]);

            // Validate results array
            if (!Array.isArray(results) || results.length < 2) {
                console.error("Invalid results from Redis operation:", results);
                return;
            }
            const [old_block, new_block] = results;
            if (!old_block) {
                console.log("No block present in old block")
                await bscProcessHelper.setBlocks(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.OLD_BLOCK_BSC, this.minBlockConfirmation, null)
            }
            if (!new_block) {
                console.log("No block present in new block")
                await bscProcessHelper.setBlocks(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.NEW_BLOCK_BSC, this.minBlockConfirmation, null)
            }
            if (old_block && new_block) {
                const oldBlock: number = Number(old_block);
                const newBlock = Number(new_block);
                if (newBlock <= oldBlock) {
                    await bscProcessHelper.setBlocks(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.NEW_BLOCK_BSC, this.minBlockConfirmation, newBlock)
                    return;
                } else {
                    const block_diff: number = newBlock - oldBlock;
                    if (block_diff > 50) {
                        await bscProcessHelper.diff_blocks_add_in_db(newBlock, oldBlock)
                        await bscProcessHelper.setKeyValuePair(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.OLD_BLOCK_BSC, newBlock.toString())
                        await bscProcessHelper.setBlocks(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.NEW_BLOCK_BSC, this.minBlockConfirmation, newBlock)
                    } else {
                        await this.readBlocks(oldBlock, newBlock)
                    }
                }
            } else {
                console.log("No old block and no new block")
                await globalHelper.saveErrorLogs('Bsc_getBlocks', 'No old block no new block')
            }
        } catch (err: any) {
            console.error("Error in getBlocks", err)
            await globalHelper.saveErrorLogs('bsc_getBlocks', err.message)
        }
    }
    public async readBlocks(blockNumber: number, toBlockNumber: number) {
        try {
            const block_updated: number = blockNumber + 1;
            await bscProcessHelper.setKeyValuePair(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.OLD_BLOCK_BSC, block_updated.toString());
            console.table({
                LATEST_BLOCK: blockNumber,
                BLOCK_DIFFERENCE: toBlockNumber - blockNumber,
                TO_BLOCK_NUMBER: toBlockNumber
            });
            await this.getTxFromBlock(blockNumber, KEYS.latestBlock);
        } catch (err: any) {
            console.error("Error in readBlocks>>.", (err as Error).message)
            await globalHelper.saveErrorLogs('BSC_readBlocks', err.message)
        }
    }
    public async getTxFromBlock(blockId: number, key: string) {
        try {
            const blockInfo = await blockChainHelper.provider.getBlock(blockId, true);
            if (!blockInfo) {
                console.log("No block info");
                await bscOldBLockModel.create({
                    block_number: blockId,
                    coin_family: config.STATIC_COIN_FAMILY.BNB,
                    start_block: blockId,
                    end_block: blockId,
                    status: 0
                });
                return;
            }
            const transactions = blockInfo.prefetchedTransactions || [];
            if (transactions.length === 0) {
                console.log("No block info transactions");
                return;
            }

            const transactionPromises = transactions.map(async (tx) => {
                const data: blocksTrnx = {
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

            // Wait for all the transaction processing to complete
            await Promise.allSettled(transactionPromises);
        } catch (err: any) {
            console.error("Error in getTxFromBlock:", (err as Error).message);
            await globalHelper.saveErrorLogs('BSC_readBehindBlock', err.message)
        }
    }
    public async readBehindBlock() {
        try {
            // REDIS BASED
            const data_exist: any = await bscProcessHelper.getKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_BSC)
            if (data_exist) {
                const data_exists = JSON.parse(data_exist);
                if (data_exists) {
                    console.log("This block exist in redis read behined")
                    let { id, start_block, end_block } = data_exists;
                    if (end_block > start_block) {
                        console.log("This block go for getTxFromBlock")
                        await this.getTxFromBlock(start_block, KEYS.behinedBlock);
                        start_block = start_block + 1;
                        const add_data = {
                            id: id,
                            start_block: start_block,
                            end_block: end_block,
                            diff: end_block - start_block
                        }
                        await bscProcessHelper.setKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_BSC, JSON.stringify(add_data))
                    } else {
                        console.log("id>>>>>>>>>>>", id)
                        await bscOldBLockModel.update({ status: 1 }, { where: { id: id } })
                        await bscProcessHelper.deleteKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_BSC)
                        await bscProcessHelper.behindBlocks()
                    }
                }
            } else {
                // DB BASED
                await bscProcessHelper.behindBlocks()
            }
        } catch (err: any) {
            console.error("Error in readBehindBlock>>", err)
            await globalHelper.saveErrorLogs('BSC_readBehindBlock', err.message)
        }
    }
    public async readSpecificBlock() {
        try {
            console.log("Entere into readSpecificBlock>>>")
            const specificBlocks: any = await bscProcessHelper.getKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_BSC)
            const specific_block: any = JSON.parse(specificBlocks);
            console.log("specific_block>>>>", specific_block)
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


}

export const bscBlocksProcess = new BSCBlockProcess();