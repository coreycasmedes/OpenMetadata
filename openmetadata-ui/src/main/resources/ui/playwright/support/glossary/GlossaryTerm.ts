/*
 *  Copyright 2024 Collate.
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
import { APIRequestContext, expect, Page } from '@playwright/test';
import { uuid } from '../../utils/common';
import { visitGlossaryPage } from '../../utils/glossary';
import { getRandomLastName } from '../../utils/user';

type ResponseDataType = {
  name: string;
  displayName: string;
  description: string;
  reviewers: unknown[];
  relatedTerms: unknown[];
  synonyms: unknown[];
  mutuallyExclusive: boolean;
  tags: unknown[];
  glossary: Record<string, string>;
  id: string;
  fullyQualifiedName: string;
};

export class GlossaryTerm {
  randomName = getRandomLastName();
  data = {
    name: `PW.${uuid()}%${this.randomName}`,
    displayName: `PW ${uuid()}%${this.randomName}`,
    description: 'A bank account number.',
    mutuallyExclusive: false,
    glossary: '',
  };

  responseData: ResponseDataType;

  constructor(glossaryName: string, name?: string) {
    this.data.glossary = glossaryName;
    this.data.name = name ?? this.data.name;
  }

  async visitPage(page: Page) {
    await visitGlossaryPage(page, this.responseData.glossary.displayName);
    const expandCollapseButtonText = await page
      .locator('[data-testid="expand-collapse-all-button"]')
      .textContent();
    const isExpanded = expandCollapseButtonText?.includes('Expand All');
    if (isExpanded) {
      const glossaryTermListResponse = page.waitForResponse(
        `/api/v1/glossaryTerms?*glossary=${this.responseData.glossary.id}*`
      );
      await page.click('[data-testid="expand-collapse-all-button"]');
      await glossaryTermListResponse;
    }
    const glossaryTermResponse = page.waitForResponse(
      `/api/v1/glossaryTerms/name/${encodeURIComponent(
        this.responseData.fullyQualifiedName
      )}?*`
    );
    await page.getByTestId(this.data.displayName).click();
    await glossaryTermResponse;

    await expect(page.getByTestId('entity-header-display-name')).toHaveText(
      this.data.displayName
    );
  }

  async create(apiContext: APIRequestContext) {
    const response = await apiContext.post('/api/v1/glossaryTerms', {
      data: this.data,
    });

    this.responseData = await response.json();

    return await response.json();
  }

  async patch(apiContext: APIRequestContext, data: Record<string, unknown>[]) {
    const response = await apiContext.patch(
      `/api/v1/glossaryTerms/${this.responseData.id}`,
      {
        data,
        headers: {
          'Content-Type': 'application/json-patch+json',
        },
      }
    );

    this.responseData = await response.json();

    return await response.json();
  }

  get() {
    return this.responseData;
  }

  async delete(apiContext: APIRequestContext) {
    const response = await apiContext.delete(
      `/api/v1/glossaryTerms/name/${encodeURIComponent(
        this.responseData.fullyQualifiedName
      )}?recursive=true&hardDelete=true`
    );

    return await response.json();
  }
}
