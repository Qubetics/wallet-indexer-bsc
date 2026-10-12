import database from "../config/db";
import { QueryTypes } from "sequelize";
import { CoinInterface, coinsModel, bscOldBLockModel, WalletInterface } from "../models";
import redisClient from "../config/redis";
import { promisify } from "util";
import { config } from "../config/config";
import { globalHelper } from "../helpers/global.helper";
import { blockChainHelper } from "../helpers";
import { Op } from "sequelize";

class BscProcessHelper {
  public CoinsInterface: CoinInterface[];
  public WalletAddresses: WalletInterface[];

  constructor() {
    this.fetchBEP20Tokens();
    this.fetchWalletAddress();
  }

  public async getKeyValuePair(keyName: string, field: string) {
    try {
      const getAsync = promisify(redisClient.HGET).bind(redisClient);
      const value = await getAsync(keyName, field.toUpperCase() || "");
      return value;
    } catch (err: any) {
      console.error('getKeyValuePair >>>>', err);
      return null;
    }
  }

  public async getMultipleKeyValuePairs(keyName: string, fields: string[]) {
    try {
      const upperFields = fields.map(f => f.toUpperCase());
      return new Promise((resolve, reject) => {
        redisClient.hmget(keyName, ...upperFields, (err, results) => {
          if (err) reject(err);
          else resolve(results);
        });
      });
    } catch (err: any) {
      console.error('getMultipleKeyValuePairs >>>>', err);
      return null;
    }
  }

  public async setBlocks(keyName: string, filed: string, minBlockConfirmation: number, newBlock: number | null) {
    try {
      let latestBlock = await blockChainHelper.provider.getBlockNumber();
      latestBlock -= minBlockConfirmation;

      const shouldUpdate = !newBlock || newBlock < latestBlock;

      if (shouldUpdate) {
        await bscProcessHelper.setKeyValuePair(keyName, filed, latestBlock.toString());
      } else {
        console.log("No update required for block number");
      }
    } catch (err: any) {
      console.error("Error in setBlocks:", err);
      await globalHelper.saveErrorLogs('BSC_setBlocks', err.message);
    }
  }

  public async setKeyValuePair(keyName: string, field: string, value: string) {
    try {
      redisClient.hset(keyName, field.toUpperCase(), value);
    } catch (err: any) {
      console.error('setKeyValuePair >>>', err);
    }
  }

  public async diff_blocks_add_in_db(current_block: number, old_block: number) {
    try {
      const dataToInsert = { block_number: old_block, coin_family: config.STATIC_COIN_FAMILY.BNB, start_block: old_block, end_block: current_block, status: 0 }
      console.log("dataToInsert>>>>", dataToInsert)
      await bscOldBLockModel.create(dataToInsert)
      return true;
    } catch (err: any) {
      console.error("💥 ~ Error in diff_blocks_add_in_redis.", err)
      await globalHelper.saveErrorLogs('BSC_diff_blocks_add_in_db', err.message)
      return false;
    }
  }

  public async deleteKeyValuePair(keyName: string, field: string) {
    try {
      redisClient.hdel(keyName, field.toUpperCase(), function (err) {
        if (err) {
          throw err;
        }
        return true;
      })
    } catch (err: any) {
      console.error("Error in deleteKeyValuePair>>>", err)
      return false;
    }
  }

  public fetchBEP20Tokens = async () => {
    try {
      const tokens: any = await coinsModel.findAll({
        attributes: ["coin_id", "coin_symbol", "coin_family", "is_token", "token_type", "decimals", "token_address", "for_swap", "added_by"],
        where: {
          coin_status: 1,
          coin_family: config.STATIC_COIN_FAMILY.BNB,
          [Op.or]: [{ token_type: { [Op.ne]: config.TOKEN_TYPE721_BSC } }, { token_type: null }]
        },
        raw: true,
      });
      
      for await (const token of tokens) {
        const findTokenData = await this.getKeyValuePair(
          `${config.TOKEN_TYPE_BSC}`,
          token.token_address == null ? 'NativeCoin'.toUpperCase() : token.token_address.toUpperCase(),
        );
        if (!findTokenData) {
          this.setKeyValuePair(
            `${config.TOKEN_TYPE_BSC}`,
            token.token_address == null ? 'NativeCoin'.toUpperCase() : token.token_address.toUpperCase(),
            JSON.stringify(token)
          );
        }
      }
    } catch (err: any) {
      console.error('fetchBEP20Tokens >>>>>', err);
      await globalHelper.saveErrorLogs('BSC_fetchBEP20Tokens', err.message)
    }
  }

  public fetchWalletAddress = async () => {
    try {
      const getaddressQuery = `SELECT ws.user_id,ws.wallet_address as address, ws.wallet_name FROM wallets ws FORCE INDEX (coin_family) JOIN coins cs ON ws.coin_id = cs.coin_id WHERE cs.coin_family = ${config.STATIC_COIN_FAMILY.BNB} GROUP BY ws.wallet_address`;
      const addresses: any = await database.db_write.query(
        getaddressQuery,
        { type: QueryTypes.SELECT }
      );
      for await (let wallet of addresses) {
        let findAddress = await this.getKeyValuePair(
          `${config.BSC_WALLET_ADDRESS}`,
          wallet.address.toUpperCase()
        );
        if (!findAddress) {
          this.setKeyValuePair(
            `${config.BSC_WALLET_ADDRESS}`,
            wallet.address.toUpperCase(),
            JSON.stringify(wallet)
          );
        }
      }
    } catch (error) {
      console.error('💥 ~ fetchWalletAddress error', error);
    }
  };

  public checkIfContract = async (address: string) => {
    try {
      const token_data: any = await this.getKeyValuePair(config.TOKEN_TYPE_BSC, address);
      return JSON.parse(token_data);
    } catch (err: any) {
      console.error('check_our_wallet_address >>>>>', err);
      return null;
    }
  };

  public check_our_wallet_address = async (address: string) => {
    try {
      const wallet_data: any = await this.getKeyValuePair(config.BSC_WALLET_ADDRESS, address);
      if (wallet_data) {
        console.log("wallet_Data>>>>>>>>", wallet_data)
      }
      return JSON.parse(wallet_data);
    } catch (err: any) {
      console.error('check_our_wallet_address >>>>>', err);
      return null;
    }
  };

  public behindBlocks = async () => {
    try {
      const behindBlocks: any = await bscOldBLockModel.findOne({
        attributes: ['id', 'block_number', 'start_block', 'end_block'],
        where: { status: 0 },
        raw: true,
        order: [['id', 'ASC']]
      })
      if (behindBlocks) {
        const add_data = {
          id: behindBlocks.id,
          start_block: behindBlocks.start_block,
          end_block: behindBlocks.end_block,
          diff: behindBlocks.end_block - behindBlocks.start_block
        }
        await this.setKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_BSC, JSON.stringify(add_data))
      } else {
        console.log("you have not any behind block yet")
      }
    } catch (err: any) {
      console.error(`behindBlocks >>>`, err);
      return null;
    }
  }
}
const bscProcessHelper = new BscProcessHelper();
export default bscProcessHelper;