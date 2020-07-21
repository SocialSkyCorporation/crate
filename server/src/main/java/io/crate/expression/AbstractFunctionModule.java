/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression;

import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionName;
import io.crate.metadata.FunctionProvider;
import io.crate.metadata.functions.Signature;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

public abstract class AbstractFunctionModule<T extends FunctionImplementation> extends AbstractModule {

    private HashMap<FunctionName, List<FunctionProvider>> functionImplementations = new HashMap<>();
    private MapBinder<FunctionName, List<FunctionProvider>> implementationsBinder;
    private Map<Integer, Signature> signatureByOid = new HashMap<>();
    private Map<FunctionName, Signature> signatureByName = new HashMap<>();
    private Set<String> schemas = new HashSet<>();


    public void register(Signature signature, BiFunction<Signature, Signature, FunctionImplementation> factory) {
        List<FunctionProvider> functions = functionImplementations.computeIfAbsent(
            signature.getName(),
            k -> new ArrayList<>());
        var duplicate = functions.stream().filter(fr -> fr.getSignature().equals(signature)).findFirst();
        if (duplicate.isPresent()) {
            throw new IllegalStateException(
                "A function already exists for signature = " + signature);
        }
        signatureByOid.put(signature.getOid(), signature);
        signatureByName.put(signature.getName(), signature);
        schemas.add(signature.getName().schema());
        functions.add(new FunctionProvider(signature, factory));
    }

    public Signature getFunctionSignatureByOid(Integer funcOid) {
        if (funcOid == null) {
            new IllegalArgumentException("function oid cannot be null");
        }
        return signatureByOid.get(funcOid);
    }

    public Signature getFunctionSignaturesByName(String funcName) {
        if (funcName == null) {
            new IllegalArgumentException("function name cannot be null");
        }
        int dot = funcName.indexOf(".");
        if (dot != -1) {
            String [] parts = funcName.split("\\.");
            if (parts.length != 2) {
                new IllegalArgumentException(String.format(
                    Locale.ENGLISH, "unrecognised function name format", funcName));
            }
            return signatureByName.get(new FunctionName(parts[0], parts[1]));
        }
        Optional<Signature> maybeSignature = schemas // [null, pg_catalog ...]
            .stream()
            .map(sch -> new FunctionName(sch, funcName))
            .map(signatureByName::get)
            .filter(Objects::nonNull)
            .findFirst();
        return maybeSignature.isPresent() ? maybeSignature.get() : null;
    }

    public abstract void configureFunctions();

    @Override
    protected void configure() {
        configureFunctions();

        implementationsBinder = MapBinder.newMapBinder(
            binder(),
            new TypeLiteral<FunctionName>() {},
            new TypeLiteral<List<FunctionProvider>>() {});
        for (Map.Entry<FunctionName, List<FunctionProvider>> entry : functionImplementations.entrySet()) {
            implementationsBinder.addBinding(entry.getKey()).toProvider(entry::getValue);
        }

        // clear registration maps
        functionImplementations = null;
    }
}
