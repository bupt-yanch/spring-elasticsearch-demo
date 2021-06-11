package com.example.demo.controller;

import com.example.demo.dao.EsEntityClient;
import com.example.demo.entity.Entity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping(value = "/entity")
public class EntityController {

    @Autowired
    private EsEntityClient esEntityClient;

    @GetMapping("/{id}")
    public Entity queryById(@PathVariable String id) {
        return esEntityClient.queryEntityById(id);
    }

    @GetMapping()
    public List<Entity> queryByIds(@RequestParam List<String> ids) {
        return esEntityClient.queryByIds(ids);
    }

    @GetMapping("/precise")
    public List<Entity> queryByName(@RequestParam String name) {
        return esEntityClient.queryEntityByName(name);
    }

    @GetMapping("/condition")
    public List<Entity> queryByCondition(@RequestParam(required = false) String name,
                                         @RequestParam(required = false) String summary,
                                         @RequestParam(required = false) String introduction) {
        return esEntityClient.query(name, summary, introduction);
    }
}
