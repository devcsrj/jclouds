/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jclouds.profitbricks.compute.extensions;

import static com.google.common.base.Preconditions.checkState;
import static org.jclouds.profitbricks.config.ProfitBricksComputeProperties.TIMEOUT_SNAPSHOT_AVAILABLE;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

import javax.annotation.Resource;
import javax.inject.Named;
import javax.inject.Singleton;

import org.jclouds.Constants;
import org.jclouds.compute.domain.CloneImageTemplate;
import org.jclouds.compute.domain.Image;
import org.jclouds.compute.domain.ImageTemplate;
import org.jclouds.compute.domain.ImageTemplateBuilder;
import org.jclouds.compute.extensions.ImageExtension;
import org.jclouds.compute.reference.ComputeServiceConstants;
import org.jclouds.logging.Logger;
import org.jclouds.profitbricks.ProfitBricksApi;
import org.jclouds.profitbricks.domain.Provisionable;
import org.jclouds.profitbricks.domain.Server;
import org.jclouds.profitbricks.domain.Snapshot;
import org.jclouds.profitbricks.domain.Storage;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import com.google.inject.Inject;

@Singleton
public class ProfitBricksImageExtension implements ImageExtension {

   @Resource
   @Named(ComputeServiceConstants.COMPUTE_LOGGER)
   protected Logger logger = Logger.NULL;

   private final Predicate<Storage> matchBootDevice = new Predicate<Storage>() {
      @Override
      public boolean apply(Storage input) {
         return input.bootDevice() == null ? false : input.bootDevice();
      }
   };

   private final ProfitBricksApi api;
   private final Predicate<String> snapshotAvailablePredicate;
   private final Function<Provisionable, Image> imageTransformer;
   private final ListeningExecutorService userExecutor;

   @Inject
   ProfitBricksImageExtension(ProfitBricksApi api,
           @Named(TIMEOUT_SNAPSHOT_AVAILABLE) Predicate<String> snapshotAvailablePredicate,
           Function<Provisionable, Image> imageTransformer,
           @Named(Constants.PROPERTY_USER_THREADS) ListeningExecutorService userExecutor) {
      this.api = api;
      this.snapshotAvailablePredicate = snapshotAvailablePredicate;
      this.imageTransformer = imageTransformer;
      this.userExecutor = userExecutor;
   }

   @Override
   public ImageTemplate buildImageTemplateFromNode(String name, String id) {
      Server server = api.serverApi().getServer(id);

      if (server == null)
         throw new NoSuchElementException("Cannot find server with id: " + id);

      List<Storage> storages = server.storages();
      if (storages.isEmpty() || !Iterables.tryFind(storages, matchBootDevice).isPresent())
         throw new NoSuchElementException("Server " + id + " does not contain a boot device");

      return new ImageTemplateBuilder.CloneImageTemplateBuilder().nodeId(id).name(name).build();
   }

   @Override
   public ListenableFuture<Image> createImage(ImageTemplate template) {
      checkState(template instanceof CloneImageTemplate, "profitbricks only supports creating images through cloning.");
      final CloneImageTemplate cloneTemplate = (CloneImageTemplate) template;
      String serverId = cloneTemplate.getSourceNodeId();

      Server server = api.serverApi().getServer(serverId);
      Storage bootDevice = Iterables.find(server.storages(), matchBootDevice);

      final Snapshot requested = api.snapshotApi().createSnapshot(
              Snapshot.Request.creatingBuilder()
              .storageId(bootDevice.id())
              .name(template.getName())
              .description(template.getName() + " (created with jclouds)")
              .build());

      return userExecutor.submit(new Callable<Image>() {
         @Override
         public Image call() throws Exception {
            if (snapshotAvailablePredicate.apply(requested.id())) {
               Snapshot built = api.snapshotApi().getSnapshot(requested.id());
               return imageTransformer.apply(built);
            }

            throw new UncheckedTimeoutException("Image was not created within the time limit: "
                    + cloneTemplate.getName());
         }
      });
   }

   @Override
   public boolean deleteImage(String id) {
      Snapshot snapshot = api.snapshotApi().getSnapshot(id);
      if (snapshot != null)
         try {
            logger.debug(">> deleting snapshot %s..", id);
            return api.snapshotApi().deleteSnapshot(id);
         } catch (Exception ex) {
            logger.error(ex, ">> error deleting snapshot %s..", id);
         }
      return true;
   }

}
