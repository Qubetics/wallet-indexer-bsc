import { config } from "../config/config";
import bscProcessHelper from "./process.helper";
import { BscOldBLockModel } from "../models";
import kafkaHelper from "../config/kafka.helper";
import { dbHelper } from "../helpers";

class ReadBlockProcess {
    public minBlockConfirmation: number;
    public swap: string | null;
    public approval: string | null;

    public bsc_block_topic = config.KAFKA_BLOCKS.BSC_BLOCK_TOPIC;
    public bsc_behind_block_topic = config.KAFKA_BLOCKS.BSC_BEHIND_BLOCK_TOPIC;


    constructor() {
        this.minBlockConfirmation = Number(config.MIN_BLOCK_CONFORMATIONS);
    }


    public async readCurrentBlocks() {
        try {
            const results = await bscProcessHelper.getMultipleKeyValuePairs(config.REDISKEYS.BSC_BLOCKS, [config.REDISKEYS.START_BLOCK, config.REDISKEYS.END_BLOCK]);
            console.log("🚀 ~ EthBlockProcess ~ readCurrentBlocks ~ results:", results)

            // Validate results array
            if (!Array.isArray(results) || results.length < 2) {
                console.error("Invalid results from Redis operation:", results);
                return;
            }
            const [startBlock, endBlock] = results;


            if (!startBlock) {
                console.log("No block present in old block")
                await bscProcessHelper.setBlocks(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.START_BLOCK, this.minBlockConfirmation, null)
            }
            if (!endBlock) {
                console.log("No block present in new block")
                await bscProcessHelper.setBlocks(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.END_BLOCK, this.minBlockConfirmation, null)
            }

            if (startBlock && endBlock) {
                const oldBlock: number = Number(startBlock);
                const newBlock: number = Number(endBlock);
                if (newBlock <= oldBlock) {
                    await bscProcessHelper.setBlocks(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.END_BLOCK, this.minBlockConfirmation, newBlock)
                } else {
                    const blockDiff: number = newBlock - oldBlock;
                    if (blockDiff > 50) {
                        await bscProcessHelper.diffBlocksAddInDb(newBlock, oldBlock)
                        await bscProcessHelper.setKeyValuePair(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.START_BLOCK, newBlock.toString())
                        await bscProcessHelper.setBlocks(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.END_BLOCK, this.minBlockConfirmation, newBlock)
                    } else {
                        const blockUpdated: number = oldBlock + 1;
                        await bscProcessHelper.setKeyValuePair(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.START_BLOCK, blockUpdated.toString());
                        console.table({
                            LATEST_BLOCK: oldBlock,
                            BLOCK_DIFFERENCE: newBlock - oldBlock,
                            TO_BLOCK_NUMBER: newBlock
                        });
                        if (oldBlock % 2 === 0) {
                            console.log('oldBlock even>>>', oldBlock);
                            kafkaHelper.produceMessage(`${this.bsc_block_topic}_even`, oldBlock);
                        } else {
                            console.log('oldBlock odd>>>', oldBlock);

                            kafkaHelper.produceMessage(`${this.bsc_block_topic}_odd`, oldBlock);
                        }
                    }
                }
            } else {
                console.log(" no old block and no new block")
                await dbHelper.saveErrorLogs('Bsc_setBlocks', 'No old block no new block')
            }
        } catch (err: any) {
            console.error("Error in getBlocks", err)
            await dbHelper.saveErrorLogs('bsc_getBlocks', err.message)
        }
    }

    public async readBehindBlock() {
        try {
            // REDIS BASED
            const dataExist: any = await bscProcessHelper.getKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_BSC);
            if (dataExist) {
                const parseDataExist = JSON.parse(dataExist);

                if (parseDataExist) {
                    const { id, start_block, end_block } = parseDataExist;
                    if (end_block > start_block) {
                        const newStartBlock = start_block + 1;
                        const updateKeyData = {
                            id: id,
                            start_block: newStartBlock,
                            end_block: end_block,
                            diff: end_block - newStartBlock
                        }
                        await bscProcessHelper.setKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_BSC, JSON.stringify(updateKeyData))

                        kafkaHelper.produceMessage(`${this.bsc_behind_block_topic}`, start_block);

                    } else {
                        await BscOldBLockModel.update({ status: 1 }, { where: { id: id } })
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
            await dbHelper.saveErrorLogs('BSC_readBehindBlock', err.message)
        }
    }


}

export const readBlockProcess = new ReadBlockProcess();