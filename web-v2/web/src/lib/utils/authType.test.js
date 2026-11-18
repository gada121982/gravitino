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

import { describe, expect, it } from 'vitest'

import { pickUiAuthType } from './authType'

describe('pickUiAuthType', () => {
  it('keeps the first authenticator when the UI knows it', () => {
    expect(pickUiAuthType(['simple', 'oauth'])).toBe('simple')
    expect(pickUiAuthType([' oauth '])).toBe('oauth')
    expect(pickUiAuthType(['basic'])).toBe('basic')
  })

  it('skips an authenticator the UI has no login flow for', () => {
    expect(pickUiAuthType(['com.example.auth.ServiceTokenAuthenticator', 'oauth'])).toBe('oauth')
    expect(pickUiAuthType(['kerberos', ' simple'])).toBe('simple')
  })

  it('falls back to the first entry when none is known', () => {
    expect(pickUiAuthType(['kerberos'])).toBe('kerberos')
    expect(pickUiAuthType([])).toBeUndefined()
    expect(pickUiAuthType(undefined)).toBeUndefined()
  })
})
