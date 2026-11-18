/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Authenticators the web UI has a login flow for.
export const UI_AUTH_TYPES = ['simple', 'basic', 'oauth']

/**
 * Pick the authenticator the web UI should log in with.
 *
 * `gravitino.authenticators` lists every authenticator the server accepts, in server order.
 * A deployment may put one first that the UI has no login flow for — for example a custom
 * `Authenticator` class for service tokens, which has to precede `oauth` because the server
 * hands a token to the first authenticator that claims it. Taking the first entry blindly
 * leaves the login page on a spinner forever, so take the first one the UI knows instead.
 * Falls back to the first entry when none is known, which keeps the previous behavior.
 *
 * @param {string[]|undefined} authenticators value of `gravitino.authenticators` from /configs
 * @returns {string|undefined} the authenticator name to drive the UI with
 */
export const pickUiAuthType = authenticators => {
  const names = (Array.isArray(authenticators) ? authenticators : []).map(name => String(name).trim())

  return names.find(name => UI_AUTH_TYPES.includes(name)) ?? names[0]
}
