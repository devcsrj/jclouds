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
package org.jclouds.profitbricks.compute.config;

import static org.jclouds.compute.config.ComputeServiceProperties.TIMEOUT_IMAGE_AVAILABLE;
import static org.jclouds.compute.config.ComputeServiceProperties.TIMEOUT_NODE_RUNNING;
import static org.jclouds.compute.config.ComputeServiceProperties.TIMEOUT_NODE_SUSPENDED;
import static org.jclouds.profitbricks.config.ProfitBricksComputeProperties.TIMEOUT_DATACENTER_AVAILABLE;
import static org.jclouds.util.Predicates2.retry;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.TimeUnit;

import javax.inject.Named;
import javax.inject.Singleton;

import org.jclouds.compute.ComputeServiceAdapter;
import org.jclouds.compute.config.ComputeServiceAdapterContextModule;
import org.jclouds.compute.domain.Hardware;
import org.jclouds.compute.domain.Image;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Volume;
import org.jclouds.compute.extensions.ImageExtension;
import org.jclouds.compute.reference.ComputeServiceConstants.PollPeriod;
import org.jclouds.compute.reference.ComputeServiceConstants.Timeouts;
import org.jclouds.compute.strategy.CreateNodesInGroupThenAddToSet;
import org.jclouds.domain.Location;
import org.jclouds.functions.IdentityFunction;
import org.jclouds.lifecycle.Closer;
import org.jclouds.location.suppliers.ImplicitLocationSupplier;
import org.jclouds.location.suppliers.implicit.OnlyLocationOrFirstZone;
import org.jclouds.profitbricks.ProfitBricksApi;
import org.jclouds.profitbricks.compute.ProfitBricksComputeServiceAdapter;
import org.jclouds.profitbricks.compute.concurrent.ProvisioningJob;
import org.jclouds.profitbricks.compute.concurrent.ProvisioningManager;
import org.jclouds.profitbricks.compute.extensions.ProfitBricksImageExtension;
import org.jclouds.profitbricks.compute.function.ProvisionableToImage;
import org.jclouds.profitbricks.compute.function.ServerToNodeMetadata;
import org.jclouds.profitbricks.compute.function.StorageToVolume;
import org.jclouds.profitbricks.compute.strategy.AssignDataCenterToTemplate;
import org.jclouds.profitbricks.domain.Provisionable;
import org.jclouds.profitbricks.domain.ProvisioningState;
import org.jclouds.profitbricks.domain.Server;
import org.jclouds.profitbricks.domain.Storage;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;

public class ProfitBricksComputeServiceContextModule extends
        ComputeServiceAdapterContextModule<Server, Hardware, Provisionable, Location> {

   @SuppressWarnings("unchecked")
   @Override
   protected void configure() {
      super.configure();

      install(new FactoryModuleBuilder().build(ProvisioningJob.Factory.class));

      bind(ImplicitLocationSupplier.class).to(OnlyLocationOrFirstZone.class).in(Scopes.SINGLETON);

      bind(CreateNodesInGroupThenAddToSet.class).to(AssignDataCenterToTemplate.class).in(Scopes.SINGLETON);

      bind(new TypeLiteral<ComputeServiceAdapter<Server, Hardware, Provisionable, Location>>() {
      }).to(ProfitBricksComputeServiceAdapter.class);

      bind(new TypeLiteral<Function<Server, NodeMetadata>>() {
      }).to(ServerToNodeMetadata.class);

      bind(new TypeLiteral<Function<Provisionable, Image>>() {
      }).to(ProvisionableToImage.class);

      bind(new TypeLiteral<Function<Storage, Volume>>() {
      }).to(StorageToVolume.class);

      bind(new TypeLiteral<Function<Hardware, Hardware>>() {
      }).to(Class.class.cast(IdentityFunction.class));
      
      bind(new TypeLiteral<ImageExtension>(){
      }).to(ProfitBricksImageExtension.class);
   }

   @Provides
   @Singleton
   @Named(TIMEOUT_DATACENTER_AVAILABLE)
   Predicate<String> provideDataCenterAvailablePredicate(
           final ProfitBricksApi api, ProfitBricksTimeouts timeouts, PollPeriod pollPeriod) {
      return retry(new DataCenterProvisioningStatePredicate(
              api, ProvisioningState.AVAILABLE),
              timeouts.dataCenterAvailable(), pollPeriod.pollInitialPeriod, pollPeriod.pollMaxPeriod, TimeUnit.SECONDS);
   }

   @Provides
   @Named(TIMEOUT_NODE_RUNNING)
   Predicate<String> provideServerRunningPredicate(final ProfitBricksApi api, Timeouts timeouts, PollPeriod pollPeriod) {
      return retry(new ServerStatusPredicate(
              api, Server.Status.RUNNING),
              timeouts.nodeRunning, pollPeriod.pollInitialPeriod, pollPeriod.pollMaxPeriod, TimeUnit.SECONDS);
   }

   @Provides
   @Named(TIMEOUT_NODE_SUSPENDED)
   Predicate<String> provideServerSuspendedPredicate(final ProfitBricksApi api, Timeouts timeouts, PollPeriod constants) {
      return retry(new ServerStatusPredicate(
              api, Server.Status.SHUTOFF),
              timeouts.nodeSuspended, constants.pollInitialPeriod, constants.pollMaxPeriod, TimeUnit.SECONDS);
   }

   @Provides
   @Singleton
   ProvisioningManager provideProvisioningManager(Closer closer) {
      ProvisioningManager provisioningManager = new ProvisioningManager();
      closer.addToClose(provisioningManager);

      return provisioningManager;
   }

   @Provides
   @Singleton
   @Named(TIMEOUT_IMAGE_AVAILABLE)
   Predicate<String> provideSnapshotAvailablePredicate(final ProfitBricksApi api, Timeouts timeouts, PollPeriod constants) {
      return retry(new SnapshotProvisioningStatePredicate(
              api, ProvisioningState.AVAILABLE),
              timeouts.imageAvailable, constants.pollInitialPeriod, constants.pollMaxPeriod, TimeUnit.SECONDS);
   }

   static class DataCenterProvisioningStatePredicate implements Predicate<String> {

      private final ProfitBricksApi api;
      private final ProvisioningState expectedState;

      public DataCenterProvisioningStatePredicate(ProfitBricksApi api, ProvisioningState expectedState) {
         this.api = checkNotNull(api, "api must not be null");
         this.expectedState = checkNotNull(expectedState, "expectedState must not be null");
      }

      @Override
      public boolean apply(String input) {
         checkNotNull(input, "datacenter id");
         return api.dataCenterApi().getDataCenterState(input) == expectedState;
      }

   }

   static class ServerStatusPredicate implements Predicate<String> {

      private final ProfitBricksApi api;
      private final Server.Status expectedStatus;

      public ServerStatusPredicate(ProfitBricksApi api, Server.Status expectedStatus) {
         this.api = checkNotNull(api, "api must not be null");
         this.expectedStatus = checkNotNull(expectedStatus, "expectedStatus must not be null");
      }

      @Override
      public boolean apply(String input) {
         checkNotNull(input, "server id");
         return api.serverApi().getServer(input).status() == expectedStatus;
      }

   }

   static class SnapshotProvisioningStatePredicate implements Predicate<String> {

      private final ProfitBricksApi api;
      private final ProvisioningState expectedState;

      public SnapshotProvisioningStatePredicate(ProfitBricksApi api, ProvisioningState expectedState) {
         this.api = checkNotNull(api, "api must not be null");
         this.expectedState = checkNotNull(expectedState, "expectedState must not be null");
      }

      @Override
      public boolean apply(String input) {
         checkNotNull(input, "snapshot id");
         return api.snapshotApi().getSnapshot(input).state() == expectedState;
      }

   }

   @Singleton
   public static class ProfitBricksTimeouts {

      @Inject
      @Named(TIMEOUT_DATACENTER_AVAILABLE)
      private String dataCenterAvailable;

      public long dataCenterAvailable() {
         return Long.parseLong(dataCenterAvailable);
      }
   }
}
