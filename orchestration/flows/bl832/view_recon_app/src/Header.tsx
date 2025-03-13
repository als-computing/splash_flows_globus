import React, { useState } from 'react';
import { Button } from './Button';
import { Tiled } from 'bluesky-web';
import 'bluesky-web/style.css';
import './header.css';

export interface HeaderProps {
  logoUrl: string;
  title: string;
  fileName: string;
  onSelect?: () => void;
}

export const Header = ({ logoUrl, title, fileName, onSelect }: HeaderProps) => {
  const [showTiledWidget, setShowTiledWidget] = useState(false);
  
  const handleSelectClick = () => {
    setShowTiledWidget(true);
    if (onSelect) {
      onSelect();
    }
  };
  
  const handleCloseModal = () => {
    setShowTiledWidget(false);
  };
  
  return (
    <header>
      <div className="storybook-header">
        <div>
          <img src={logoUrl} className="header-logo" alt="Logo"/>
          <h1>{title}</h1>
          <div className="header-file-name">{fileName}</div>
        </div>
        <div>
          <Button size="small" label="Select Dataset" onClick={handleSelectClick}/>
        </div>
      </div>
      
      {/* Modal for TiledWidget */}
      {showTiledWidget && (
        <div className="modal-overlay">
          <div className="modal-content">
            <div className="modal-header">
              <h3>Select a Dataset</h3>
              <button className="close-button" onClick={handleCloseModal}>×</button>
            </div>
            <div className="modal-body">
              <div className="tiled-container">
                <Tiled />
              </div>
            </div>
          </div>
        </div>
      )}
    </header>
  );
};