package io.sofastack.stockmng.impl;

import io.sofastack.stockmng.facade.StockMngFacade;
import io.sofastack.stockmng.mapper.StockMngMapper;
import io.sofastack.stockmng.model.ProductInfo;
import io.sofastack.stockmng.model.Success;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import io.serverless_lib.RpcHandle;
import io.serverless_lib.RpcHandleOwner;
import io.serverless_lib.UdsBackend;

import javax.annotation.PostConstruct;
// import com.alipay.sofa.runtime.api.annotation.SofaService;
// import com.alipay.sofa.runtime.api.annotation.SofaServiceBinding;

// @Service
// @SofaService(interfaceType = StockMngFacade.class, uniqueId = "${service.unique.id}", bindings = { @SofaServiceBinding(bindingType = "bolt") })

// class RpcHandlerImpl implements RpcHandler{
//     @Resource 
//     StockMngImpl rpcDispatch;

//     HashMap<String,RpcFunc> handleMeta;

//     String appName(){
//         return "stock-mng";
//     }

//     ByteBuf handleRpc(String func,ByteBuf req){
//         // get meta

//         // deserialize

//         // call

//         // serialize
//     }
// }

@Service
public class StockMngImpl implements StockMngFacade {
    @Resource
    private StockMngMapper stockMngMapper;

    @Autowired
    RpcHandleOwner rpcHandleOwner;

    @PostConstruct
    public void init() {
        rpcHandleOwner.register((StockMngFacade) this);
    }

    @Override
    public Success createUser(String userName) {
        Success success = new Success();
        success.setSuccess("true");
        return success;
    }

    @Override
    public List<ProductInfo> query(String userName) {
        Integer stockRecordCount = stockMngMapper.getStockRecordCountForUserName(userName);
        if (stockRecordCount == null) {
            initUser(userName);
        }

        return stockMngMapper.query(userName);
    }

    private void initUser(String userName) {
        for (int i = 0; i < 5; i++) {
            stockMngMapper.insertStockRecord(
                    "0000" + (i + 1),
                    DatabaseSeed.name[i],
                    DatabaseSeed.description[i],
                    DatabaseSeed.price[i],
                    10000,
                    userName);
        }
    }

    @Override
    public BigDecimal queryProductPrice(String productCode, String userName) {
        return stockMngMapper.queryProductPrice(productCode, userName);
    }

    @Override
    public boolean createOrder(String userName, String productCode, int count) {
        return stockMngMapper.createOrder(userName, productCode, count) > 0;
    }

    @Override
    public boolean minusStockCount(String userName, String productCode, int count) {
        return stockMngMapper.minusStockCount(userName, productCode, count) > 0;
    }
}
