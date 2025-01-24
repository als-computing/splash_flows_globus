import React from 'react';

import { Button } from './Button';
import './header.css';

export interface HeaderProps {
  logoUrl: string;
  onSelect?: () => void;
}

export const Header = ({ logoUrl, onSelect }: HeaderProps) => (
  <header>
    <div className="storybook-header">
      <div>
      <img src={logoUrl} className="header-logo"/>
        <h1>Tomography Visualizer powered by itk-vtk-viewer</h1>
      </div>
      <div>
        <Button size="small" label="Select Dataset" onClick={onSelect}/>
      </div>
    </div>
  </header>
);