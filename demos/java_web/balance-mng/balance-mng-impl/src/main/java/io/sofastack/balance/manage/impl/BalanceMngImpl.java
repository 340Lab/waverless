package io.sofastack.balance.manage.impl;

import io.sofastack.balance.manage.facade.BalanceMngFacade;
import io.sofastack.balance.manage.mapper.BalanceMngMapper;
import io.sofastack.balance.manage.model.Balance;
import javax.annotation.Resource;
import java.math.BigDecimal;

import org.springframework.stereotype.Service;

public class BalanceMngImpl implements BalanceMngFacade {
    @Resource
    private BalanceMngMapper balanceMngMapper;

    @Override
    public void createUser(String userName) {
        Balance balance = balanceMngMapper.userExists(userName);
        if (balance == null) {
            balanceMngMapper.createUser(userName);
        }
    }

    @Override
    public BigDecimal queryBalance(String userName) {
        Balance balance = balanceMngMapper.queryBalance(userName);
        if (balance == null) {
            throw new RuntimeException("user name does not exist");
        }
        return balance.getBalance();
    }

    @Override
    public void minusBalance(String userName, BigDecimal amount) {
        balanceMngMapper.minusBalance(userName, amount);
    }
}
