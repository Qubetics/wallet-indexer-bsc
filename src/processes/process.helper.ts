import database from "../config/db";
import Sequelize, { QueryTypes } from "sequelize";
import { CoinInterface, CoinsModel, BscOldBLockModel, WalletInterface } from "../models";
import redisClient from "../config/redis";
import { promisify } from "util";
import { config } from "../config/config";
import { BooleanEnum, CoinFamilyEnum } from "../enum";
import { global_helper } from "../helpers/global_helper";
import { Blockchain_Helper } from "../helpers";
import { Op } from "sequelize";

class BscProcessHelper {
  public CoinsInterface: CoinInterface[];
  public WalletAddresses: WalletInterface[];

  constructor() {
    this.fetchERC20Tokens();
    this.fetchWalletAddress();
  }

  public getKeyValuePair = async (keyName: string, field: string) => {
    try {
      const getAsync = promisify(redisClient.HGET).bind(redisClient);
      const value = await getAsync(keyName, field.toUpperCase() || "");
      return value;
    } catch (err: any) {
      console.error('getKeyValuePair >>>>', err);
      return null;
    }
  }

  public setBlocks = async (field: string, key: string, minBlockConfirmation: number, newBlock: number | null) => {
    try {
      let latestBlock = await Blockchain_Helper.provider.getBlockNumber();
      console.log('latestBlock <<<', latestBlock);
      console.log('minBlockConfirmation <<<', minBlockConfirmation);
      console.log('key <<<', key);
      latestBlock = latestBlock - minBlockConfirmation;
      if (newBlock) {
        if (newBlock < latestBlock) {
          await bscProcessHelper.setKeyValuePair(field, key, latestBlock.toString())
        } else {
          console.log("No updation of blocks")
        }
      } else {
        await bscProcessHelper.setKeyValuePair(field, key, latestBlock.toString())
      }
    } catch (err: any) {
      console.error("Error in  getBlocks", err)
      await global_helper.save_error_logs('MATIC_setBlocks', err.message)
    }
  }

  public setKeyValuePair = async (keyName: string, field: string, value: string) => {
    try {
      redisClient.hset(keyName, field.toUpperCase(), value);
    } catch (err: any) {
      console.error('setKeyValuePair >>>', err);
    }
  }

  public diff_blocks_add_in_db = async (current_block: number, old_block: number) => {
    try {
      let value_exist: any = await this.getKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.NODE_ALERT_BSC)
      if (value_exist) {
        let setvalue: number = Number(value_exist) + 1;
        if (Number(value_exist) <= 100) {
          await this.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.NODE_ALERT_BSC, setvalue.toString())
          if (Number(value_exist) == 0 || Number(value_exist) == 5 || Number(value_exist) == 10 || Number(value_exist) == 20 || Number(value_exist) == 50 || Number(value_exist) == 75 || Number(value_exist) == 100) {
            let email_body = `<p>Bscereum Node Issue ${Number(value_exist)}</p><p>Current block running = ${current_block}.</p><p>Old Block = ${old_block}.</p>`;
            await global_helper.send_mail(config.NODE_ISSUE_ALERT_EMAIL, email_body, `Bsc Node Issue ${Number(value_exist)}`);
          } else {
            console.log("no email")
          }
        } else {
          console.log("Number(value_exist) > 100")
        }
      } else {
        await this.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.NODE_ALERT_BSC, '0')
      }
      let dataToInsert = { block_number: old_block, coin_family: config.STATIC_COIN_FAMILY.BNB, start_block: old_block, end_block: current_block, status: 0 }
      console.log("dataToInsert>>>>", dataToInsert)
      await BscOldBLockModel.create(dataToInsert)
      return true;
    } catch (err: any) {
      console.error("💥 ~ Error in diff_blocks_add_in_redis.", err)
      await global_helper.save_error_logs('BSC_diff_blocks_add_in_db', err.message)
      return false;
    }
  }

  public getHashTable = async (keyName: string) => {
    try {
      const getAsync: any = promisify(redisClient.hvals).bind(redisClient)
      const value: any = await getAsync(keyName)
      return value;
    } catch (err: any) {
      console.error("Error in getHashTable>>>", err)
      return null;
    }
  }

  public deleteKeyValuePair = async (keyName: string, field: string) => {
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

  public fetchWalletAddress = async () => {
    try {
      const getaddressQuery = `SELECT ws.user_id,ws.wallet_address as address, ws.wallet_name FROM wallets ws FORCE INDEX (coin_family) JOIN coins cs ON ws.coin_id = cs.coin_id WHERE cs.coin_family = ${config.STATIC_COIN_FAMILY.BNB} GROUP BY ws.wallet_address`;
      const addresses: any = await database.db_write.query(
        getaddressQuery,
        { type: QueryTypes.SELECT }
      );
      for await (let wallet of addresses) {
        let findAddress = await bscProcessHelper.getKeyValuePair(
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
      console.error('💥 ~ fetchAllWalletAddress error', error);
    }
  };

  public fetchERC20Tokens = async () => {
    try {
      let tokens: any = await CoinsModel.findAll({
        attributes: ["coin_id", "coin_symbol", "coin_family", "is_token", "token_type", "decimals", "token_address", "for_swap", "added_by"],
        where: {
          coin_status: 1,
          coin_family: config.STATIC_COIN_FAMILY.BNB,
          [Op.or]: [{ token_type: { [Op.ne]: config.TOKEN_TYPE721_BSC } }, { token_type: null }]
        },
        raw: true
      });
      for await (let token of tokens) {
        let findTokenData = await this.getKeyValuePair(
          `${config.TOKEN_TYPE_BSC}`,
          token.token_address.toUpperCase()
        );
        if (!findTokenData) {
          this.setKeyValuePair(
            `${config.TOKEN_TYPE_BSC}`,
            token.token_address == null ? 'NativeCoin'.toUpperCase() : token.token_address.toUpperCase(),
            JSON.stringify(token)
          );
        }
      }
      // redisClient.set(config.TOKEN_TYPE_BSC || "", JSON.stringify(tokens));
    } catch (err: any) {
      console.error('fetchERC20Tokens >>>>>', err);
      await global_helper.save_error_logs('BSC_fetchERC20Tokens', err.message)
    }
  }

  public checkIfContract = async (address: string) => {
    try {
      let token_data: any = await this.getKeyValuePair(config.TOKEN_TYPE_BSC, address);
      if (token_data) {
        console.log("token_data>>>>>>>>", token_data)
      }
      return JSON.parse(token_data);
    } catch (err: any) {
      console.error('check_our_wallet_address >>>>>', err);
      return null;
    }
  };

  // public checkIfContract = async (address: string) => {
  //   try {
  //     const getAsync = promisify(redisClient.get).bind(redisClient);
  //     const value = await getAsync(config.TOKEN_TYPE_BSC || "");

  //     const erc20Tokens: CoinInterface[] = value ? JSON.parse(value) : [];
  //     const token = erc20Tokens.find(
  //       (el: any) => Number(el.is_token) === Number(BooleanEnum.true) && el.token_address?.toUpperCase() === address?.toUpperCase()
  //     );
  //     if (token) return token;
  //     return null;
  //   } catch (error: any) {
  //     console.error('checkIfContract >>>', error);
  //     return null;
  //   }
  // };

  public fetchERC721Tokens = async () => {
    try {
      const getTokenQuery = `SELECT * FROM coins WHERE coin_status=1 AND coin_family=${config.STATIC_COIN_FAMILY.BNB} AND (token_type!='${config.TOKEN_TYPE_BSC}' OR token_type is null)`;

      const tokens: CoinInterface[] = await database.db_read.query(getTokenQuery, {
        type: Sequelize.QueryTypes.SELECT,
      });

      redisClient.set(config.TOKEN_TYPE721_BSC, JSON.stringify(tokens));
    } catch (error: any) {
      console.error('fetchERC721Tokens >>>>', error);
    }
  };

  // public checkIfOurNFTContract = async (address: string) => {
  //   try {
  //     const getAsync = promisify(redisClient.get).bind(redisClient);
  //     const value = await getAsync(config.TOKEN_TYPE721_BSC);
  //     const erc721Tokens: CoinInterface[] = value ? JSON.parse(value) : [];
  //     const token = erc721Tokens.find(
  //       (el: any) => Number(el.is_token) === Number(BooleanEnum.true) && el.token_address.toUpperCase() === address?.toUpperCase()
  //     );

  //     if (token) return token;
  //     return null;
  //   } catch (error: any) {
  //     console.error('checkIfOurNFTContract >>>>>', error);
  //     return null;
  //   }
  // };

  public check_our_wallet_address = async (address: string) => {
    try {
      let wallet_data: any = await this.getKeyValuePair(config.BSC_WALLET_ADDRESS, address);
      if (wallet_data) {
        console.log("wallet_Data>>>>>>>>", wallet_data)
      }
      return JSON.parse(wallet_data);
    } catch (err: any) {
      console.error('check_our_wallet_address >>>>>', err);
      return null;
    }
  };

  // public NativeCoinByCoinFamily = async (coin_family: CoinFamilyEnum) => {
  //   try {
  //     const getAsync = promisify(redisClient.get).bind(redisClient);
  //     const value = await getAsync(config.TOKEN_TYPE_BSC || "");
  //     const erc20Tokens: CoinInterface[] = value ? JSON.parse(value) : [];
  //     return erc20Tokens.find(
  //       (el: CoinInterface) => Number(el.is_token) === Number(BooleanEnum.false) && el.coin_family === coin_family
  //     );

  //   } catch (err: any) {
  //     console.error(`NativeCoinByCoinFamily >>>`, err);
  //     return 0;
  //   }
  // }

  // public CoinByCoinId = async (coin_id: number) => {
  //   try {
  //     const getAsync = promisify(redisClient.get).bind(redisClient);
  //     const value = await getAsync(config.TOKEN_TYPE_BSC || "");

  //     const erc20Tokens: CoinInterface[] = value ? JSON.parse(value) : [];
  //     return erc20Tokens.find(
  //       (el: CoinInterface) => el.coin_id === coin_id
  //     );

  //   } catch (err: any) {
  //     console.error(`CoinByCoinId >>>`, err);
  //     return null;
  //   }
  // }

  public behindBlocks = async () => {
    try {
      let behindBlocks: any = await BscOldBLockModel.findOne({
        attributes: ['id', 'block_number', 'start_block', 'end_block'],
        where: { status: 0 },
        raw: true,
        order: [['id', 'ASC']]
      })
      if (behindBlocks) {
        let add_data = {
          id: behindBlocks.id,
          start_block: behindBlocks.start_block,
          end_block: behindBlocks.end_block,
          diff: behindBlocks.end_block - behindBlocks.start_block
        }
        let dataaaaa_exist: any = await this.setKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_BSC, JSON.stringify(add_data))
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